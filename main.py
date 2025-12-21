import pandas as pd
import requests
import io
import random
from sqlalchemy import create_engine
from datetime import datetime

from FlightRadar24 import FlightRadar24API  # Nowa biblioteka!

# --- KONFIGURACJA ---
DB_FILENAME = "turystyka.db"

print("--- START: POZYSKIWANIE DANYCH Z FLIGHTRADAR24 (UNOFFICIAL) ---")


# ==============================================================================
# 1. BAZA WIEDZY (SZACOWANIE PASAŻERÓW)
# ==============================================================================
# Uproszczona logika: Jeśli kod samolotu zaczyna się na 'B73' to Boeing 737 itp.
def get_capacity(aircraft_code):
    if not aircraft_code:
        return 150
    ac = aircraft_code.upper()
    if "A38" in ac:
        return 525
    if "B74" in ac:
        return 416
    if "B77" in ac or "A35" in ac:
        return 350
    if "B78" in ac or "A33" in ac:
        return 290
    if "A321" in ac or "B739" in ac:
        return 220
    if "A32" in ac or "B73" in ac:
        return 180
    if "E19" in ac or "CRJ" in ac:
        return 100
    return 150  # Domyślna średnia


TOURIST_AIRLINES = ["RYR", "WZZ", "EZY", "TOM", "CFG", "NAX", "NKS", "VLG", "ENT", "LS"]


# ==============================================================================
# 2. POBIERANIE SŘOWNIKA GEOGRAFICZNEGO
# ==============================================================================
def get_geo_mapping():
    print("1. [EXTRACT] Pobieranie mapy lotnisk (OpenFlights)...")
    url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
    cols = ["ID", "Name", "City", "Country", "IATA", "ICAO", "Lat", "Lon", "Alt", "Tz", "Dst", "TzDb", "Type", "Source"]
    try:
        response = requests.get(url)
        df = pd.read_csv(io.StringIO(response.text), header=None, names=cols)
        # Bierzemy tylko potrzebne kolumny. FlightRadar używa kodów ICAO (4 litery) i IATA (3 litery)
        # Zrobimy mapowanie po IATA, bo jest popularniejsze w raportach
        df_clean = df[df["IATA"] != "\\N"][["IATA", "City", "Country", "Lat", "Lon", "Name"]].copy()
        print(f"   -> Załadowano słownik dla {len(df_clean)} lotnisk.")
        return df_clean
    except Exception as e:
        print(f"   -> BŘĄD POBIERANIA GEOGRAFII: {e}")
        return pd.DataFrame()


# ==============================================================================
# 2'. DODATKOWA FUNKCJA: POBIERANIE DANYCH O LUDNOŚCI MIAST
# ==============================================================================
def get_city_population_mapping():
    print("1b. [EXTRACT] Pobieranie danych o ludności miast...")
    url = "https://raw.githubusercontent.com/condwanaland/worldcities/refs/heads/main/data-raw/worldcities.csv"

    try:
        response = requests.get(url)
        df = pd.read_csv(io.StringIO(response.text))

        df = df[["city_ascii", "country", "population"]].copy()
        df["city_ascii"] = df["city_ascii"].str.lower().str.strip()
        df["country"] = df["country"].str.lower().str.strip()

        df["population"] = pd.to_numeric(df["population"], errors="coerce")

        print(f"   -> Załadowano dane dla {len(df)} miast.")
        return df
    except Exception as e:
        print(f"   -> BŘĄD POBIERANIA DANYCH O LUDNOŚCI: {e}")
        return pd.DataFrame()


# ==============================================================================
# 3. SCRAPING FLIGHTRADAR24
# ==============================================================================
def fetch_flightradar_data():
    print("2. [EXTRACT] Łączenie z siecią FlightRadar24...")
    fr_api = FlightRadar24API()

    # Pobieramy listę wszystkich lotów (to może chwilę potrwać)
    # Możesz dodać filtr regionu, np. bounds=fr_api.get_bounds('Europe')
    try:
        flights = fr_api.get_flights()
        print(f"   -> Wykryto {len(flights)} samolotów w powietrzu na całym świecie.")
    except Exception as e:
        print(f"   -> Błąd połączenia z FR24: {e}")
        return pd.DataFrame()

    data_list = []

    print("   -> Przetwarzanie i filtrowanie danych (tylko loty z ustalonym celem)...")
    count = 0
    for f in flights:
        # Interesują nas tylko loty, które mają lotnisko docelowe (dest)
        # FR24 często zwraca kod lotniska w formacie IATA (3 litery) lub ICAO (4 litery).
        # Biblioteka udostępnia property: destination_airport_iata

        if f.destination_airport_iata and f.destination_airport_iata != "N/A":
            data_list.append({
                    "snapshot_utc": datetime.utcnow(),
                    "flight_number": f.callsign,
                    "airline_icao": f.airline_icao,
                    "aircraft_code": f.aircraft_code,
                    "origin_iata": f.origin_airport_iata,
                    "dest_iata": f.destination_airport_iata,
                    "altitude": f.altitude,
                    "ground_speed": f.ground_speed,
                }
            )
            count += 1

            # Opcjonalnie: Limit dla testów, żeby nie przetwarzać 10,000 rekordów
            # if count >= 3000: break

    df = pd.DataFrame(data_list)
    print(f"   -> Gotowe. Znaleziono {len(df)} aktywnych lotów pasażerskich/cargo z ustaloną trasą.")
    return df


# ==============================================================================
# 4. ANALIZA I ZAPIS
# ==============================================================================

# A. Pobieramy dane
df_geo = get_geo_mapping()
df_flights = fetch_flightradar_data()

if not df_flights.empty:
    print("3. [TRANSFORM] Wzbogacanie danych (Obliczanie pasażerów)...")

    enriched_rows = []
    for _, row in df_flights.iterrows():
        # Szacowanie pojemności
        seats = get_capacity(row["aircraft_code"])

        # Szacowanie czy to turysta (na podstawie kodu linii ICAO)
        # Np. RYR (Ryanair), WZZ (WizzAir)
        airline_code = str(row["airline_icao"]).upper()
        is_tourist = airline_code in TOURIST_AIRLINES

        if is_tourist:
            load_factor = 0.95
            tourist_prob = 90
        else:
            load_factor = 0.82
            tourist_prob = 40

        est_pax = int(seats * load_factor)

        enriched_rows.append({"Est_Passengers": est_pax, "Tourist_Probability": tourist_prob})

    df_enriched = pd.concat([df_flights, pd.DataFrame(enriched_rows)], axis=1)

    print("4. [MERGE] Řączenie z bazą miast...")
    # Řączymy po kodzie IATA
    df_final = pd.merge(
        df_enriched,
        df_geo,
        left_on="dest_iata",
        right_on="IATA",
        how="inner",  # Zostawiamy tylko te, które udało się zmapować na miasto
    )

    # Wybieramy ładne kolumny
    final_cols = [
        "flight_number",
        "airline_icao",
        "aircraft_code",
        "City",
        "Country",
        "Name",
        "dest_iata",
        "Lat",
        "Lon",
        "Est_Passengers",
        "Tourist_Probability",
    ]
    df_final = df_final[final_cols]
    df_final.rename(columns={"Name": "Airport_Name"}, inplace=True)

    # Dodanie populacji miasta
    df_final["City_norm"] = df_final["City"].str.lower().str.strip()
    df_final["Country_norm"] = df_final["Country"].str.lower().str.strip()

    df_cities = get_city_population_mapping()

    df_final = pd.merge(
        df_final, df_cities, left_on=["City_norm", "Country_norm"], right_on=["city_ascii", "country"], how="left"
    )

    df_final.drop(columns=["city_ascii", "country", "City_norm", "Country_norm"], inplace=True)
    df_final.rename(columns={"population": "City_Population"}, inplace=True)

    print(f"5. [LOAD] Zapisywanie {len(df_final)} rekordów do bazy '{DB_FILENAME}'...")
    engine = create_engine(f"sqlite:///{DB_FILENAME}")

    # df_final.to_sql("live_traffic", con=engine, if_exists="replace", index=False)
    df_final.to_sql(
        "flight_snapshots",
        con=engine,
        if_exists="append",
        index=False
    )


    print("\n--- SUKCES! TOP 10 KIERUNKÓW W TEJ CHWILI ---")
    with engine.connect() as conn:
        print(
            pd.read_sql(
                "SELECT City, Country, COUNT(*) as Loty, SUM(Est_Passengers) as Ludzie FROM live_traffic GROUP BY City ORDER BY Loty DESC LIMIT 10",
                conn,
            )
        )

        print("\n--- TOP 10 KIERUNKÓW POD WZGLĘDEM PASAŻERÓW NA MIESZKAŘCA ---")
        print(
            pd.read_sql(
                "SELECT City, Country, SUM(Est_Passengers) AS Ludzie, City_Population, ROUND(1.0 * SUM(Est_Passengers) / City_Population, 6) AS Podróżni_na_mieszkańca FROM live_traffic WHERE City_Population IS NOT NULL GROUP BY City ORDER BY Podróżni_na_mieszkańca DESC LIMIT 10;",
                conn,
            )
        )

else:
    print("Brak danych z FlightRadar.")

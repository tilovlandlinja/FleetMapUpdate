import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pyodbc
import json

with open('credentials.json') as f:
    credentials = json.load(f)

server = credentials["db_server"]  # eller IP-adresse
database = credentials["db_name"]
dbusername = credentials["db_user"]
dbpassword = credentials["db_password"]

def upsert_driver(cursor, vehicle):
    logged_in = vehicle.get("LoggedInPerson")
    if not logged_in:
        return None

    driver_external_id = logged_in.get("Id")
    name = logged_in.get("Name") or ""
    phone = logged_in.get("Phone") or ""
    employee_id = logged_in.get("Id") or 0
    vehicle_id = logged_in.get("VehicleId") or None
    vehicle_logged_in_since = logged_in.get("VehicleLoggedInSinceUtc")
    position = logged_in.get("PropertiesString") or "Ukjent"
    now = datetime.now()

    sql = """
        MERGE INTO Drivers AS target
        USING (SELECT ? AS DriverExternalId) AS source
        ON target.DriverExternalId = source.DriverExternalId
        WHEN MATCHED AND target.PreventUpdate = 0 THEN 
            UPDATE SET 
                Name = ?, Email = ?, PhoneNumber = ?, UpdatedAt = ?
        WHEN NOT MATCHED THEN
            INSERT (DriverExternalId, Name, Email, PhoneNumber, Position, BaseLocation, CreatedAt, UpdatedAt, company, IsMontor)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'eviny', 0);
        """

    params = [
        employee_id,
        name,
        '',
        phone,
        datetime.now(),  # UpdatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
        employee_id,
        name,
        '',
        phone,
        'Driver',  # Position – ingen naturlig mapping, sett til 'Driver' eller en annen verdi hvis ønskelig
        'Unknown',  # BaseLocation – ingen naturlig mapping, sett til 'Unknown' eller en annen verdi hvis ønskelig
        datetime.now(),  # CreatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
        datetime.now(),  # UpdatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
    ]


    try:
        cursor.execute(sql, params)
        cursor.execute("SELECT Id FROM Drivers WHERE DriverExternalId = ? and company = 'eviny'", (driver_external_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"❌ [upsert_driver] Feil for driver '{driver_external_id}': {e}")
        return None

def upsert_vehicle(cursor, vehicle, driverid):

    print(f"Upsert vehicle: {vehicle}")
    print(f"Driver ID: {driverid}")
    now = datetime.now()
    asset_id = str(vehicle.get("Id") or "")
    license_plate = vehicle.get("RegNumber") or ""
    make = vehicle.get("Name") or ""  # Mapping fra 'Name' i JSON
    company = vehicle.get("Company") or ""

    status = 'inactive'
    if vehicle.get("Status",{}).get("Id") == 1:
        status = 'active'

    pos = vehicle.get("LastPosition", {}) or {}
    lat = pos.get("Latitude")
    lon = pos.get("Longitude")
    timestamp = pos.get("TimestampUtc")
    speed = pos.get("SpeedKmh", 0)
    bearing = pos.get("Bearing", 0)

    sql = """
    MERGE INTO AbaxVehicles AS target
    USING (SELECT ? AS AssetId) AS source
    ON target.AssetId = source.AssetId 
    WHEN MATCHED THEN
        UPDATE SET
            LicensePlateNumber = ?, VehicleMake = ?,
            DriverId = ?, Status = ?, 
            Latitude = ?, Longitude = ?, Speed = ?, Direction = ?, 
            LocationTimestamp = ?, LastUpdated = ?
    WHEN NOT MATCHED THEN
        INSERT (AssetId, LicensePlateNumber, VehicleMake,
                DriverId, Status, 
                Latitude, Longitude, Speed, Direction, 
                LocationTimestamp, LastUpdated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    params = [
        asset_id,
        license_plate,
        make,
        driverid,
        status,
        lat,
        lon,
        speed,
        bearing,
        timestamp,
        now,
        asset_id,
        license_plate,
        make,
        driverid,
        status,
        lat,
        lon,
        speed,
        bearing,
        timestamp,
        now
    ]

    try:
        print(f"Upsert kjøretøy {asset_id} med sjåfør {driverid}")
        cursor.execute(sql, params)
    except Exception as e:
        print(f"❌ [upsert_vehicle] Feil for kjøretøy {asset_id}: {e}")

# API-informasjon
url = credentials["eviny_api_url"]
brukernavn = credentials["eviny_username"]
passord = credentials["eviny_password"]

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={dbusername};"
    f"PWD={dbpassword};"
    f"TrustServerCertificate=yes;"
)

TEST_MODE = False

conn = pyodbc.connect(conn_str, autocommit=False)
cursor = conn.cursor()

response = requests.get(url, auth=HTTPBasicAuth(brukernavn, passord))

if response.status_code == 200:
    data = response.json()

    for vehicle in data:
        if not vehicle.get("LoggedInPerson"):
            continue

        

        logged_in = vehicle.get("LoggedInPerson") or {}
        properties = logged_in.get("Properties") or []

        #print(f"Properties: {properties}")

        if "Energimontør" not in properties:
            # gjør noe
            continue
        
        #print(vehicle)

        driverid = upsert_driver(cursor, vehicle)
        print(f"Driver ID: {driverid}")
        if not driverid:
            print(f"❌ Ingen sjåfør funnet for {vehicle.get('RegNumber')}")
            continue
        try:
            driverid = upsert_driver(cursor, vehicle)
            upsert_vehicle(cursor, vehicle, driverid)
            conn.commit()  # Commit etter hvert kjøretøy
            print(f"✅ Oppdatert kjøretøy {vehicle.get('RegNumber')} med sjåfør {driverid}")
        except Exception as e:
            print(f"Feil i transaksjon: {e}")
            conn.rollback()

        #print(f"🔍 Ville oppdatert {vehicle.get('RegNumber')} med sjåfør {vehicle['LoggedInPerson'].get('Name')}")

    """ if TEST_MODE:
        print("🧪 TESTMODE: Ruller tilbake alle endringer.")
        conn.rollback()
    else:
        conn.commit()
        print("✅ Endringer lagret i databasen.") """
else:
    print(f"❌ API-feil: {response.status_code} - {response.text}")

cursor.close()
conn.close()

import pandas as pd
import pyodbc
from datetime import datetime, timezone, timedelta
import json

with open('credentials.json') as f:
    credentials = json.load(f)

server = credentials["db_server"]  # eller IP-adresse
database = credentials["db_name"]
dbusername = credentials["db_user"]
dbpassword = credentials["db_password"]

csv_file = "nordvest.csv"

def read_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    """Les CSV-fil og returner en DataFrame."""
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin1', low_memory=False)
        return df
    except Exception as e:
        print(f"Feil ved lesing av CSV-fil: {e}")
        return pd.DataFrame()  # Returner tom DataFrame ved feil

def upsert_driver(cursor, driver):
    sql = """
    MERGE INTO Drivers AS target
    USING (SELECT ? AS DriverExternalId) AS source
    ON target.DriverExternalId = source.DriverExternalId
    WHEN MATCHED THEN 
        UPDATE SET 
            Name = ?, Email = ?, PhoneNumber = ?, UpdatedAt = ?, company = 'Nordvestnett'
    WHEN NOT MATCHED THEN
        INSERT (DriverExternalId, Name, Email, PhoneNumber, Position, BaseLocation, isMontor, CreatedAt, UpdatedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    params = [
        driver.get('external_id'),
        driver.get('name'),
        driver.get('email'),
        driver.get('phone_number'),
        datetime.now(),  # UpdatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
        driver.get('external_id'),
        driver.get('name'),
        driver.get('email'),
        driver.get('phone_number'),
        'Montør',  # Position – ingen naturlig mapping, sett til 'Driver' eller en annen verdi hvis ønskelig
        'Unknown',  # BaseLocation – ingen naturlig mapping, sett til 'Unknown' eller en annen verdi hvis ønskelig
        1,  # isMontor – antatt at alle er montører, sett til 1 (True)
        datetime.now(),  # CreatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
        datetime.now(),  # UpdatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
    ]

    #return id for driver
    cursor.execute(sql, params)
     # Så hent ID eksplisitt
    sql_select = "SELECT Id FROM Drivers WHERE DriverExternalId = ?"
    cursor.execute(sql_select, (driver.get('external_id'),))
    row = cursor.fetchone()
   
    return row[0] if row else None


def upsert_vehicle(cursor, vehicle, driverid):
    """ import pprint
    from datetime import datetime """

    """ print("== START upsert_vehicle ==") """

    # Debug: vis AssetId og driverId
    """ print("AssetId:", vehicle.get('asset_id'))
    print("DriverId:", driverid) """

    # Finn eksisterende kjøretøy på fører
    sql_driverscar = "SELECT * FROM AbaxVehicles WHERE DriverId = ?"
    cursor.execute(sql_driverscar, driverid)
    row = cursor.fetchone()

    """ if row:
        print("🚗 Eksisterende kjøretøy funnet for driver:", row)
    else:
        print("ℹ️ Ingen eksisterende kjøretøy funnet for driver.") """

    # Tidsstempel for oppdatering
    updatedtime = vehicle.get('location', {}).get('timestamp')

    # Alternativ: parse timestamp til datetime hvis ønskelig
    # if updatedtime:
    #     updatedtime = datetime.fromisoformat(updatedtime.replace("Z", "+00:00"))
    # else:
    #     updatedtime = datetime.now()

    sql = """MERGE INTO AbaxVehicles AS target
    USING (SELECT ? AS AssetId) AS source
    ON target.AssetId = source.AssetId
    WHEN MATCHED THEN 
        UPDATE SET 
            LicensePlateNumber = ?, VehicleMake = ?, VehicleModel = ?, Vin = ?, SerialNo = ?,
            Latitude = ?, Longitude = ?, Speed = ?, Direction = ?, 
            Odometer = ?, LocationTimestamp = ?, Notes = ?, 
            FuelType = ?, FuelConsumption = ?, Color = ?, DriverId = ?, Accuracy = ?, IsMoving = ?, LastUpdated = ?, SignalSource = ?, Type = ?
    WHEN NOT MATCHED THEN
        INSERT (AssetId, LicensePlateNumber, VehicleMake, VehicleModel, Vin, SerialNo,
                Latitude, Longitude, Speed, Direction, 
                Odometer, LocationTimestamp, Notes, 
                FuelType, FuelConsumption, Color, DriverId, Accuracy, IsMoving, LastUpdated, SignalSource, Type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    # Parameterliste
    params = [
        # For MATCH
        vehicle.get('asset_id'),
        vehicle.get('license_plate', {}).get('number'),
        vehicle.get('manufacturer', {}).get('name'),
        vehicle.get('model', {}).get('name'),
        vehicle.get('vin'),
        vehicle.get('unit', {}).get('serial_number') if vehicle.get('unit') else None,
        vehicle.get('location', {}).get('latitude', 0),
        vehicle.get('location', {}).get('longitude', 0),
        0,
        vehicle.get('location', {}).get('course'),
        (vehicle.get('odometer', {}).get('value', 0) / 1000) if vehicle.get('odometer') else None,
        vehicle.get('location', {}).get('timestamp'),
        None,
        vehicle.get('fuel_type'),
        vehicle.get('fuel_consumption'),
        vehicle.get('color'),
        driverid,
        vehicle.get('location', {}).get('accuracy', 0),
        int(vehicle.get('location', {}).get('in_movement', 0)),  # BIT/BOOLEAN må være 0/1
        updatedtime,
        vehicle.get('location', {}).get('signal_source'),
        vehicle.get('unit', {}).get('type') if vehicle.get('unit') else None,

        # For INSERT
        vehicle.get('asset_id'),
        vehicle.get('license_plate', {}).get('number'),
        vehicle.get('manufacturer', {}).get('name'),
        vehicle.get('model', {}).get('name'),
        vehicle.get('vin'),
        vehicle.get('unit', {}).get('serial_number') if vehicle.get('unit') else None,
        vehicle.get('location', {}).get('latitude', 0),
        vehicle.get('location', {}).get('longitude', 0),
        0,
        vehicle.get('location', {}).get('course'),
        (vehicle.get('odometer', {}).get('value', 0) / 1000) if vehicle.get('odometer') else None,
        vehicle.get('location', {}).get('timestamp'),
        None,
        vehicle.get('fuel_type'),
        vehicle.get('fuel_consumption'),
        vehicle.get('color'),
        driverid,
        vehicle.get('location', {}).get('accuracy', 0),
        int(vehicle.get('location', {}).get('in_movement', 0)),
        updatedtime,
        vehicle.get('location', {}).get('signal_source'),
        vehicle.get('unit', {}).get('type') if vehicle.get('unit') else None
    ]

    # Debug: print parametre
    #print("🧾 Parametre til MERGE:")
    #pprint.pprint(params)

    try:
        cursor.execute(sql, params)
        #print("✅ MERGE kjørt OK")
    except Exception as e:
        print("❌ Feil under MERGE:", e)

    #print("== SLUTT upsert_vehicle ==")

def main():

    # 🔌 1. DB-tilkobling og henting av nye data
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={dbusername};"
        f"PWD={dbpassword};"
        f"TrustServerCertificate=yes;"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()


    df = read_csv_to_dataframe(csv_file)
    if df.empty:
        print("Ingen data funnet i CSV-filen.")
        return

    for index, row in df.iterrows():
        # Skriv ut hver rad i DataFrame
        print(f"Rad {index + 1}:")

        driver = {
            'external_id' : row.get('Telefon'),
            'name': row.get('Montør'),
            'email': str( row.get('Telefon') ) + '@nordvestnett.no',
            'phone_number': row.get('Telefon'),
            'company': row.get('Selskap'),
        }

        vehicle = {
            'asset_id': str( row.get('Telefon') ),
            'license_plate': {
                'number': ''
            },
            'manufacturer': {
                'name': ''
            },
            'model': {
                'name': ''
            },
            'vin': '',
            'unit': {
                'serial_number': '',
                'type': 'CsvImport'
            },
            'location': {
                'latitude': row.get('Nord'),
                'longitude': row.get('Øst'),
                'timestamp': datetime.now(),
                'course': 0,
                'accuracy': 0,
                'in_movement': 0,
                'signal_source': 'CsvImport'
            },
            'odometer': {
                'value': 0
            },
            'fuel_type': '',
            'fuel_consumption': '',
            'color': ''
        }

        driverid = upsert_driver(cursor, driver)

        upsert_vehicle(cursor, vehicle, driverid)

        
        print(vehicle)

    conn.commit()
    cursor.close()
    conn.close()
    """ for index, row in df.iterrows():
        # Skriv ut hver rad i DataFrame
        print(f"Rad {index + 1}:")
        for column, value in row.items():
            print(f"  {column}: {value}")
        print() """

if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
from zeep import Client
import pyodbc
import datetime

# Konfigurasjon
server = '10.182.86.36,1433'  # eller IP-adresse
database = 'FleetAdminDb'
dbusername = 'FleetDb'
dbpassword = 'Sixemas-51'

abax_username = "morenett-api"
abax_password = "Morenett14"
wsdl_url = "https://ws.track2find.com/ws/external/api/ExternalApiWebService.asmx?WSDL"


def safe(value, default=None):
    return value if value is not None else default



def upsert_vehicle(cursor, vehicle):
    sql = """
        UPDATE AbaxVehicles
        SET Status = ?
        WHERE SerialNo = ? AND (Status IS NULL OR Status != ?)
    """

    new_status = safe(vehicle.get('status'), 0)
    serial_no = safe(vehicle.get('serial_number'), '')

    params = [new_status, serial_no, new_status]
    cursor.execute(sql, params)

    if cursor.rowcount > 0:
        print(f"🔄 Oppdaterte Status for SerialNo {serial_no} → {new_status}")
    #else:
        #print(f"✅ Status for SerialNo {serial_no} var allerede {new_status}")

def upsert_vehicle2(cursor, vehicle, driverid):
    sql = """
        UPDATE AbaxVehicles SET 
            Latitude = ?, Longitude = ?, Direction = ?, 
            LocationTimestamp = ?, LastUpdated = ?, Status = ?
        WHERE Serialno = ?
    """

    now = datetime.datetime.now()

    params = [
        safe(vehicle['latitude'], 0),
        safe(vehicle['longitude'], 0),
        safe(vehicle['direction'], 0),
        vehicle['timestamp'],
        now,
        safe(vehicle['status'], 0),
        safe(vehicle['serial_number'], 0)
    ]

    cursor.execute(sql, params)

def main():
    try:
        # Koble til SOAP
        client = Client(wsdl=wsdl_url)
        session_id = client.service.Login(abax_username, abax_password)

        # Hent posisjoner med tilgjengelig sjåfør
        positions = client.service.GetTriplogUnitPositionsDriverAvailable(session_id)
    except Exception as e:
        print(f"Feil ved oppkobling eller henting fra SOAP-tjenesten: {e}")
        return
    
    if not positions:
        print("Ingen posisjoner funnet.")
        return
    # Koble til databasen
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={dbusername};PWD={dbpassword}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for pos in positions:
        try:        
            if( pos.SerialNo == 'MUS300881'  or pos.SerialNo == 'ANS195357' ):
                print( pos )
            status = 'active' if pos.DriverAvailable == 1 else 'inactive'

            vehicle_data = {
                'serial_number' : pos.SerialNo, 
                'status': status
            }
            #print(f"Behandler kjøretøy {pos.SerialNo} status {status} {pos.DriverAvailable}")
            upsert_vehicle(cursor, vehicle_data)
            conn.commit()
        except Exception as e:
            print(f"Feil ved behandling av kjøretøy {pos.SerialNo}: {e}")

    #conn.commit()
    cursor.close()
    conn.close()
    print("Import fra ABAX SOAP API fullført.")


if __name__ == "__main__":
    main()

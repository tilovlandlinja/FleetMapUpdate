import pyodbc
from arcgis.gis import GIS
from arcgis.features import FeatureLayer, Feature
from datetime import datetime, timezone, timedelta
import json
from typing import List, Dict
import requests
from zoneinfo import ZoneInfo
import time


with open('credentials.json') as f:
    credentials = json.load(f)

server = credentials["db_server"]  # eller IP-adresse
database = credentials["db_name"]
dbusername = credentials["db_user"]
dbpassword = credentials["db_password"]


def get_driver_map_points(cursor) -> List[Dict]:
    """ conn = pyodbc.connect(connection_string)
    cursor = conn.cursor() """
    today = datetime.now().date()
    
    now_str = datetime.now().replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Oslo")).strftime('%Y-%m-%d %H:%M:%S')
    
    print( f"Now string {now_str}")

    """ now = datetime.now(timezone.utc)
    one_hour_ago = now + timedelta(hours=2)

    print(f"one_hour_ago: {one_hour_ago}") """

    sql = """
        WITH RankedVehicles AS (
        SELECT 
            v.Id,
            v.AssetId,
            d.Name,
            d.PhoneNumber,
            d.Position,
            d.BaseLocation,
            d.DriverExternalId,
            d.Email,
            d.CreatedAt,
            d.AccessGroupName,
            d.AccessGroupShort,
            d.isMontor,
            d.Courses,
            d.company,
            v.Latitude,
            v.Longitude,
            v.Direction,
            v.LastUpdated,
            v.Type,
            v.Color,
            v.LicensePlateNumber,
            v.VehicleMake,
            v.VehicleModel,
            v.Status,
            GETUTCDATE() as testdate,
            (
                SELECT COUNT(*) 
                FROM DriverSchedules s
                WHERE s.DriverExternalId = d.DriverExternalId
                AND s.IsDeleted = 0
                AND s.StartTime <=  ?
                AND s.EndTime >= ?
            ) AS HasShift,
            (
                SELECT TOP 1 s.VaktLocation
                FROM DriverSchedules s
                WHERE s.DriverExternalId = d.DriverExternalId
                AND s.IsDeleted = 0
                AND s.StartTime <= ?
                AND s.EndTime >= ?
                ORDER BY s.StartTime
            ) AS VaktLocation,
            ROW_NUMBER() OVER (PARTITION BY d.Id ORDER BY v.LastUpdated DESC) AS rn
        FROM AbaxVehicles v
        INNER JOIN Drivers d ON d.Id = v.DriverId
        WHERE v.Latitude > 61 AND v.Longitude != 0 AND d.IsMontor = 1
    )

    SELECT *
    FROM RankedVehicles
    WHERE rn = 1
    ORDER BY LastUpdated DESC;
    """

    cursor.execute(sql, [ now_str , now_str, now_str, now_str ] )
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    # Nåværende tidspunkt i UTC
    now = datetime.now(timezone.utc  )

    print(f"now: {now}")

    # Konverter til timestamp i ms
    now_ms = int(now.timestamp() * 1000)

    print(f"now_ms: {now_ms}")
    # Nåværende tidspunkt i UTC
    now = datetime.now(timezone.utc)
    one_hour_ago = now - timedelta(hours=2)
    print(f"one_hour_ago: {one_hour_ago}")
   
    result = []
    for row in rows:
        record = dict(zip(columns, row))
        last_updated = record["LastUpdated"]

        """ if last_updated:
            if last_updated.tzinfo is None:
                last_updated = last_updated.replace(tzinfo=timezone.utc)
                #record["isActive"] = last_updated >= one_hour_ago
                record["isActive"] = last_updated >= one_hour_ago
                #record["isActive"] = False
        else:
            record["isActive"] = False """
        #print(f"last_updated: {last_updated}")
        #record["isActive"] = last_updated >= one_hour_ago
        record["hasShift"] = record["HasShift"] > 0
        if record["hasShift"] > 0 :
             record["hasShift"] = 1
             record["isActive"] = 1
        else:
            record["isActive"] = record["Status"] == "active"   
        
        
        result.append(record)

    #conn.close()
    return result

def get_access_token(client_id, client_secret, token_url):
    payload = {
        'grant_type': 'client_credentials',
        'scope': 'open_api open_api.vehicles',
        'client_id': client_id,
        'client_secret': client_secret
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.post(token_url, data=payload, headers=headers)

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def get_data_from_abax(api_key, url):
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }
    response = requests.get(url, headers=headers)   
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def upsert_driver(cursor, driver):
    sql = """
    MERGE INTO Drivers AS target
    USING (SELECT ? AS DriverExternalId) AS source
    ON target.DriverExternalId = source.DriverExternalId
    WHEN MATCHED AND target.PreventUpdate = 0 THEN 
        UPDATE SET 
            Name = ?, Email = ?, PhoneNumber = ?, UpdatedAt = ?, company = 'Linja'
    WHEN NOT MATCHED THEN
        INSERT (DriverExternalId, Name, Email, PhoneNumber, Position, BaseLocation, CreatedAt, UpdatedAt, company )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Linja');
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
        'Driver',  # Position – ingen naturlig mapping, sett til 'Driver' eller en annen verdi hvis ønskelig
        'Unknown',  # BaseLocation – ingen naturlig mapping, sett til 'Unknown' eller en annen verdi hvis ønskelig
        datetime.now(),  # CreatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
        datetime.now(),  # UpdatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
    ]

    #return id for driver
    #cursor.execute(sql, params)
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

def sync_arcgis(layer, fresh_data):
    """
    Oppdaterer ArcGIS-laget basert på fresh_data:
    - Hvis feature finnes i fresh_data (basert på DriverExternalId + AssetId):
        → Oppdater alle endrede felt + geometri
    - Hvis ikke:
        → Sett visible = False
    """
    objectid_field = layer.properties.objectIdField

    # Lag oppslag for fresh_data basert på kombinasjon av driver + assetid
    fresh_index = {
        f"{d.get('AssetId')}": d
        #f"{d.get('DriverExternalId')}_{d.get('AssetId')}": d
        for d in fresh_data
        if d.get("DriverExternalId") and d.get("AssetId")
    }

    
    # Hent alle eksisterende features fra laget
    existing_features = layer.query(where="1=1", out_fields="*", return_geometry=True).features

    existing_keys = {
        str(feature.attributes.get('assetid'))
        for feature in existing_features
        if feature.attributes.get('assetid') is not None
    }

    updates = []
    new_features = []
    delete_features = []

    for feature in existing_features:
        #key = f"{feature.attributes.get('driverExternalId')}_{feature.attributes.get('assetid')}"
        key = f"{feature.attributes.get('assetid')}"
        #print(f"key: {key}")
        current_visible = feature.attributes.get("visible", True)
        if key in fresh_index:
            d = fresh_index[key]
            changed = False
            utc_time = d.get("LastUpdated")
            # Konverter til lokal norsk tid (CEST/CET)
            local_time = utc_time.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Oslo"))
            if feature.attributes['lastUpdated'] is None:
                continue
            #print( f"lastUpdated: {utc_time} {existing.attributes['lastUpdated']} {local_time} {millisec}" )
            try:
                dt1 = datetime.fromtimestamp(feature.attributes['lastUpdated'] / 1000, tz=timezone.utc)
                dt1_ago = dt1 + timedelta(minutes=1)
            except Exception as e:
                print(f"Error converting lastUpdated: {e}")
                
            if utc_time > dt1_ago.replace(tzinfo=None):
                print( f"lastUpdated: {utc_time} {dt1_ago} {local_time}" )
                changed = True
            if feature.attributes.get('isActive') != d.get("isActive"):
                print( f"active: {feature.attributes.get('isActive')} {d.get('isActive')}" )
                changed = True
            if feature.attributes.get('hasShift') != d.get("hasShift"):
                print( f"hasShift: {feature.attributes.get('hasShift')} {d.get('hasShift')}" )
                changed = True
            """ if feature.attributes.get('driverExternalId') != d.get("DriverExternalId"):
                print( f"driverExternalId: {feature.attributes.get('driverExternalId')} {d.get('DriverExternalId')}" )
                changed = True """
            if feature.attributes.get('visible') == 0:
                print( f"visible: {feature.attributes.get('visible')} {d.get('DriverExternalId')}" )
                changed = True
            if( d.get('hasShift') ):
                print( f"hasShift: {d.get("Name")} {feature.attributes.get('hasShift')} {d.get('hasShift')}" )
            #changed = True
            if changed:
                #print( f"existing: {d.get('Name')} {d.get('PhoneNumber')} {d.get('Direction')}   {utc_time.isoformat() if local_time else None} Er aktiv {d.get("isActive")} har vakt {d.get("HasShift")}" )
                #print(existing)

                # Sjekk på tilgangstype
                access = (d.get("AccessGroupShort") or "").lower()
                area_validity = (d.get("AreaOfValidity") or "").lower()

                is_safety_leader = "lfs" in access
                can_work_alone = "lfs" in access or "afa" in access
                is_learner = "lærling" in area_validity


                if( feature.attributes.get('id') is None ):
                    feature.attributes['id'] = feature.attributes.get(objectid_field)

                feature.attributes['name'] = d.get("Name")
                feature.attributes['accessGroup'] = d.get("AccessGroupName")
                feature.attributes['phone'] = d.get("PhoneNumber")[-8:]
                feature.attributes['email'] = d.get("Email")
                feature.attributes['position'] = d.get("Position")
                feature.attributes['direction'] = d.get("Direction")
                feature.attributes['lastUpdated'] = local_time
                feature.attributes['assetid'] = d.get("AssetId")
                feature.attributes['isActive'] = d.get("isActive")
                feature.attributes['hasShift'] = d.get("hasShift")
                feature.attributes['driverExternalId'] = d.get("DriverExternalId")
                feature.attributes['isMontor'] = d.get("isMontor")
                feature.attributes['courses'] = d.get("Courses")
                feature.attributes['baseLocation'] = d.get("BaseLocation")
                feature.attributes['color'] = d.get("Color")
                feature.attributes['vehicleMake'] = d.get("VehicleMake")
                feature.attributes['vehicleModel'] = d.get("VehicleModel")
                feature.attributes['licensePlateNumber'] = d.get("LicensePlateNumber")
                feature.attributes['vaktLocation'] = d.get("VaktLocation")
                #feature.attributes['visible'] = 1
                feature.attributes['isSafetyLeader'] = is_safety_leader
                feature.attributes['canWorkAlone'] = can_work_alone
                feature.attributes['isLearner'] = is_learner
                feature.attributes['company'] = d.get("company")

                feature.geometry = {
                    "x": d.get("Longitude"),
                    "y": d.get("Latitude"),
                    "spatialReference": {"wkid": 4326}
                }

                if( d.get("Longitude") >  8 or d.get("Latitude") <  61 ):
                    #delete_features.append(feature.attributes[objectid_field])
                    feature.attributes['visible'] = False
                else:
                    feature.attributes['visible'] = True

                """ if( feature.attributes[objectid_field] > 30000 ):
                    print( f"Oppdaterer feature med id {feature.attributes[objectid_field]}:" )
                    print( f"feature.isMontor: {feature.attributes.get("isMontor")}" )
                    print( f"feature.isActive: {feature.attributes.get("isActive")}" )
                    print( f"feature.hasShift: {feature.attributes.get("hasShift")}" )
                    print( f"feature.visible: {feature.attributes.get("visible")}" )
                    print( f"feature.isSafetyLeader: {feature.attributes.get("isSafetyLeader")}" )
                    print( f"feature.canWorkAlone: {feature.attributes.get("canWorkAlone")}" )
                    print( f"feature.isLearner: {feature.attributes.get("isLearner")}" )
                    print( f"feature.id: {feature.attributes.get("id")}" )
                    print( f"feature.geometry: {feature.geometry}" )
                else: """
                updates.append(feature)

        elif feature.attributes['company'] != "Nordvestnett":
            # Ikke i fresh_data → skjul (sett visible = False) hvis nødvendig
            delete_features.append(feature.attributes[objectid_field])

            print(f"Ikke i fresh_data: {feature.attributes.get('name')} {feature.attributes.get('assetid')}")
            """ if current_visible:
                updates.append(Feature(attributes={
                    objectid_field: feature.attributes[objectid_field],
                    "visible": 0
                })) """

    for asset_id, d in fresh_index.items():
        asset_key = str(d.get("AssetId"))
        #asset_key = "faeaecd61d15465d9a1b19400fff72cb"

        if asset_key not in existing_keys:
            print(f"Ny bil: {d.get('Name')} med assetid {asset_key}")
            new_feature = Feature(
                attributes={
                    "name": d.get("Name"),
                    "accessGroup": d.get("AccessGroupName"),
                    "phone": d.get("PhoneNumber")[-8:] if d.get("PhoneNumber") else "",
                    "direction": d.get("Direction"),
                    "lastUpdated": d.get("LastUpdated").replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Oslo")),
                    "assetid": d.get("AssetId"),
                    "driverExternalId": d.get("DriverExternalId"),
                    "isActive": d.get("isActive"),
                    "hasShift": d.get("hasShift"),
                    "vaktLocation": d.get("VaktLocation"),
                    "visible": 1
                },
                geometry={
                    "x": d.get("Longitude"),
                    "y": d.get("Latitude"),
                    "spatialReference": {"wkid": 4326}
                }
            )
            #print(new_feature)
            new_features.append(new_feature)
    
    

    # 🚀 Utfør oppdateringer
    if new_features:
        print(f"🆕 Legger til {len(new_features)} nye features...")
        #result = layer.edit_features(adds=new_features)
        result_add = layer.edit_features(adds=new_features)
        #print("✅ Ferdig:", result_add)
    if delete_features:
        print(f"🗑️ Sletter {len(delete_features)} features...")

        delete_ids = ",".join(
            [str(id) for id in delete_features]
        )
        print(delete_ids)
        result_delete = layer.edit_features(deletes=delete_ids)
        #print("✅ Ferdig:", result_delete)
    if updates:
        #print(f"🔄 Oppdaterer {len(updates)} features...")
        #pprint.pprint(updates, indent=2)
        result = layer.edit_features(updates=updates)
        #print("✅ Raw resultat:")
        #pprint.pprint(result, indent=2)

        # 3) Gå gjennom hver oppdatering og plukk ut feilmeldingene
        for res in result.get('updateResults', []):
            if not res.get('success', False):
                obj_id = res.get('objectId')
                err = res.get('error', {})
                code = err.get('code')
                desc = err.get('description')
                details = err.get('details', [])
                print(f"🔴 Feil på objectId={obj_id}: kode={code}, melding='{desc}'")
                if details:
                    # detaljer kan være liste med mer info
                    for d in details:
                        print("   •", d)

        # Hvis alt ble suksess:
        success_count = sum(1 for r in result.get('updateResults', []) if r.get('success'))
        print(f"🟢 Ferdig: {success_count} av {len(updates)} oppdateringer OK.")
        
    else:
        print("✅ Ingen endringer nødvendig.")

def main():

     # Define your credentials and URLs
    client_id = credentials["abax_client_id"]
    client_secret = credentials["abax_client_secret"]
    token_url = credentials["abax_token_url"]
    api_url = credentials["abax_api_url"]

    # Get access token
    access_token = get_access_token(client_id, client_secret, token_url)
    if not access_token:
        print("Failed to obtain access token.")
        return
    
    # Get data from Abax
    data = get_data_from_abax(access_token, api_url)
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    if not data:
        print("Failed to retrieve data from Abax.")
        return
    
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

    for vehicle in data['items']:
        driver = vehicle.get('driver')
        if not driver or not driver.get('external_id'):
            continue
        try:
            driverid = upsert_driver(cursor, driver)
            upsert_vehicle(cursor, vehicle, driverid)
            conn.commit()
        except Exception as e:
            print(f"Feil ved oppdatering av {driver.get('external_id')}: {e}")
            conn.rollback()
    
    #conn.commit()
    
    #return "kake"
    # Hent data fra databasen
    fresh_data = get_driver_map_points(cursor)

    cursor.close()
    conn.close()

    # 🔐 2. Logg inn til ArcGIS
    gis = GIS("https://www.arcgis.com", credentials["arcgis_username"], credentials["arcgis_password"])

    # 2. Finn Feature Layeren du vil oppdatere
    #layer_url = f"https://services7.arcgis.com/SFrQXRodUOT24x4N/arcgis/rest/services/driver_montor_map/FeatureServer"  # Dette er URLen til Feature Layeren du vil oppdatere
    #feature_layer = FeatureLayer(layer_url)

    # 2. Hent Feature Layer
    item = gis.content.get(credentials["arcgis_layerid"])  # ← Sett inn AGOL Layer ID
    layer: FeatureLayer = item.layers[0]

    sync_arcgis(layer, fresh_data)
    
    #print("Oppdatering fullført.")

if __name__ == "__main__":
    time.sleep(5)

    main()

    """ while True:
        main()
        # kjør koden din her...
        time.sleep(60)  # vent ett minutt """
        

import requests
import json
import pyodbc
from datetime import datetime
import re
import time


with open('credentials.json') as f:
    credentials = json.load(f)

server = credentials["db_server"]  # eller IP-adresse
database = credentials["db_name"]
dbusername = credentials["db_user"]
dbpassword = credentials["db_password"]

apiurl = credentials["card_api_url"]  # URL til REST API
username = credentials["card_api_user"]  # Brukernavn for API
password = credentials["card_api_password"]  # Passord for API



def get_all_employees():
    #url = apiurl + "employees?includeHired=true&includeExternal=false"
    url = "https://www.sikkerhetskort.no/rest/v2/employees?includeExternal=true"
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(username, password)
    }
    response = requests.get(url, headers=headers, auth=(username, password))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None
    
def get_courses( cource_id = 'cc782c81-3e7d-4357-b931-8affc0f79aee' ):

    #url = apiurl + "courses/all"
    url = f"{apiurl}coursecompletions/course/{cource_id}"
    #url = apiurl + "dump?includeDeleted=false&includeHired=false&includeExternal=false"
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(username, password)
    }
    response = requests.get(url, headers=headers, auth=(username, password))

    if response.status_code == 200:
        data = response.json()
        #print( json.dumps(data, indent=4) )
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def get_access_code(cards):
    
    # Ekstraher alle shortName fra approvals
    short_names = []
    areaOfValidity = ''
    shortname = ''
    description = ''
    for card in cards:
        areaOfValidity = card.get('areaOfValidity')
        for approval in card.get("approvals", []):
            if "shortName" in approval:
                short_names.append(approval["shortName"])

    # Fjern duplikater hvis ønskelig
    unique_short_names = list(set(short_names))
    #print(f"Unique short names: {unique_short_names}")

    if areaOfValidity and 'Lærling trinn 1' in areaOfValidity:
            print(unique_short_names)
            shortname = 'GP1'
            description = 'Lærling trinn 1'

    if 'LfS' in unique_short_names:
        shortname = 'LfS'
        description = 'Leder for sikkerhet'
    elif  'AfA' in unique_short_names:
        shortname = 'AfA'
        description = 'Ansvarlig for arbeid'
    elif 'GP2' in unique_short_names:
        shortname = 'GP2'
        description = 'Godkjent som person nr.2'
    elif 'AT' in unique_short_names:
        if areaOfValidity and 'Lærling trinn 1' in areaOfValidity:
            shortname = 'AT'
            description = 'Lærling trinn 1'
        else:
            shortname = 'AT'
            description = 'Adgangstillatelse'
    else:
        print("No matching short names found")

    #print(unique_short_names)
    return ( shortname, description )

def get_saftey_card(employee_number):

    #print(f"Getting safety card for employee: {employee_number}")
    url = apiurl + f"safetycard/getByEmployee/{employee_number}"
    #url = apiurl + f"employee/{employee_number}"

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(username, password)
    }
    response = requests.get(url, headers=headers, auth=(username, password))

    #print(f"Response: {response.status_code} - {response.text}")

    if response.status_code == 200 and response.json():
        #print(json.dumps(response.json(), indent=4))
        return get_access_code(response.json())
        #print(f"Safety card: {response.json()[0].get('employeeEmail')}")
        #return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return (None, None)
    
def normalize_phone(phone):
    if not phone:
        return ''
    digits = re.sub(r'\D', '', phone)
    return  digits[-8:] if len(digits) >= 8 else digits

def normalize_email(email):
    if not email:
        return ''
    
    email = email.strip().lower()
    username = email.split('@')[0]  # tar kun det som er før @
    return f"{username}@"

def upsert_driver( cursor, driver, courses ):

    conditions = []
    params = []

    # E-post
    email = driver.get('email')
    # EmployeeNumber
    employee_number = driver.get('employeeNumber')
    # Telefonnummer
    phone = driver.get('mobilePhone')
    if phone:
        phone_last8 = normalize_phone(phone)
        conditions.append("PhoneNumber LIKE ?")
        params.append('%' + phone_last8)
    elif email:
        conditions.append("LOWER(Email) like LOWER(?)")
        email = normalize_email(email)
        params.append( email + '%')
    elif employee_number:
        conditions.append("DriverExternalId = ?")
        params.append(employee_number)

    # Bygg spørringen
    if conditions:
        check_sql = f"""
        SELECT id, PreventUpdate FROM Drivers
        WHERE {" OR ".join(conditions)};
        """
        
        MAX_RETRIES = 3
        for attempt in range(MAX_RETRIES):
            try:
                cursor.execute(check_sql, tuple(params))
                break
            except pyodbc.Error as e:
                if 'deadlocked' in str(e):
                    print(f"Deadlock, forsøker på nytt ({attempt+1})...")
                    time.sleep(1)
                else:
                    raise
        row = cursor.fetchone()

        if not row:
            #print("Fant ingen sjåfør med oppgitte kriterier.")
            return None

        driver_id, prevent_flag = row
        if prevent_flag == 1:
            print(f"Oppdatering stoppet: PreventUpdate = 1 for sjåfør {driver_id}.")
            return None
    else:
        print("Ingen identifiserbare verdier oppgitt.")
        driver_id = None
        return None

    shortname, description = get_saftey_card(driver.get('employeeId'))

    dateofbirth = driver.get('birthDate')
    DateTimeBirthdate = None
    if dateofbirth:
        DateTimeBirthdate = datetime.strptime(dateofbirth, "%Y-%m-%d")

    
    if  driver.get('title') and  (  'montør' in driver.get('title').lower()  or 'lærling' in driver.get('title').lower() ) and 'tele' not in driver.get('title').lower():
        isMontor = 1
    else:
        isMontor = 0
    #print(f"Is montor: {isMontor}")

    sql = """
    UPDATE Drivers
    SET 
        DateOfBirth = ?, 
        Position = ?, 
        BaseLocation = ?,
        AccessGroupName = ?,
        AccessGroupShort = ?,
        UpdatedAt = ?,
        IsMontor = ?,
        Courses = ?,
        Email = CASE 
            WHEN (Email IS NULL OR LTRIM(RTRIM(Email)) = '') THEN ? 
            ELSE Email 
        END
    WHERE Id = ?;
    """

    params = [
        DateTimeBirthdate,
        driver.get('title') if driver.get('title') else "Unkown",
        driver.get('departmentName') if driver.get('departmentName') else "Unkown",
        description if description else "Unkown",
        shortname if shortname else "Unkown",
        datetime.now(),  # UpdatedAt – ingen naturlig mapping, sett til nåværende tidspunkt
        isMontor,  # IsMontor – ingen naturlig mapping, sett til 0 eller 1 basert på sjåførens tittel
        courses if courses else "",
        driver.get('email') if driver.get('email') else "Unkown",  # Bruker email eller mobilnummer som DriverExternalId
        driver_id # WHERE bruker employeeNumber som DriverExternalId
    ]
    
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            cursor.execute(sql, params)
            break
        except pyodbc.Error as e:
            if 'deadlocked' in str(e):
                print(f"Deadlock, forsøker på nytt ({attempt+1})...")
                time.sleep(1)
            else:
                raise
    #cursor.execute(sql, params)

    

    print(f"Updated driver with ID: {driver.get('employeeId')}")

    """ cards = get_saftey_card(driver.get('employeeId'))

    if not cards:
        print(f"Error: No safety card found for employee {driver.get('employeeId')}")
        return None

    print(f"Safety card: {cards[0].get('employeeEmail')}")

    if 'gunnar' in cards[0].get('employeeEmail'):
        card_number = cards[0].get('safetyCardNumber')
        print(json.dumps(cards, indent=4)) """

def main():

    employees = get_all_employees()

    courcesids = [
        { 'Lastebil' : 'cc782c81-3e7d-4357-b931-8affc0f79aee'},  # Lastebil
        { 'Drone' : 'a0f2b8c1-3e7d-4a5b-bc6f-9d0e1f2c3a4b' },  # Drone
        { 'Store aggregat' : '86d5c218-49f8-4498-8fdf-f2ff7a21ad23' },  # Store aggregat
        { 'Saftey cut' : '9ef9a6d9-cf37-417b-a7ab-6c9260f4414d' } ,  # Safety cut
        { 'Korgbil' : '8fc4907e-df3c-4ae2-8e61-da55a189bc11'},  # Korgbil
        { 'Tilgang Statnett'  : 'fd8ffbaa-9e65-41f8-8960-bcbcd73bccf5' },  # Statnett
    ]

    allcourses = [] # Henter alle kurs for å sjekke om det er noen endringer

    for course in courcesids:
        for key, value in course.items():
            print(f"Getting course: {key} with ID: {value}")
            allcourses.append( { key : get_courses(value) } )

    lastebil = get_courses('cc782c81-3e7d-4357-b931-8affc0f79aee')
    drone = get_courses('a0f2b8c1-3e7d-4a5b-bc6f-9d0e1f2c3a4b') #Drone
    store_aggregat = get_courses('86d5c218-49f8-4498-8fdf-f2ff7a21ad23') #Store aggregat
    safety_cut = get_courses('9ef9a6d9-cf37-417b-a7ab-6c9260f4414d') # Safety cut
    korgbil = get_courses('9ef9a6d9-cf37-417b-a7ab-6c9260f4414d') # Korgbil

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={dbusername};"
        f"PWD={dbpassword}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    #print(json.dumps(employees, indent=4))
    if employees:
        for employee in employees:
            
            #if employee.get('firstName') != 'Mads':
            #print(employee.get('companyName'))
            #if  'Nordvest Nett AS' not in employee.get('companyName'):
            """ if 'Nordvest Nett AS' not in employee.get('companyName'):
                continue
            if  'Joachim' not in employee.get('firstName'):
                continue """

            #print(f"Processing employee: {employee.get('firstName')} {employee.get('lastName')} {employee.get('email')}, ID: {employee.get('mobilePhone') }")
            # print( employee) 
            if employee.get('email') and employee.get('companyName') == 'LINJA AS':
                courses = ""
                driver_id = employee.get('employeeId')

                kompetansarray = []

                for course in allcourses:
                    if any(entry["employeeId"] == driver_id for entry in course.get(list(course.keys())[0], [])):
                        kompetansarray.append(list(course.keys())[0])

                if( len(kompetansarray) > 0 ):
                    courses = ', '.join(kompetansarray)
                    print(f"Courses found for employee: {kompetansarray}, ID: {driver_id}")

                """ harlastebil = any(entry["employeeId"] == driver_id for entry in lastebil)
                hardrone = any(entry["employeeId"] == driver_id for entry in drone)
                harstore_aggregat = any(entry["employeeId"] == driver_id for entry in store_aggregat)
                harsafty_cut = any(entry["employeeId"] == driver_id for entry in safety_cut) """


                """ if 'tore' in employee.get('email'):
                    print(json.dumps(employee, indent=4))
                    print(f"Processing employee: {employee.get('email')}, ID: {employee.get('employeeNumber')}")
                    card = get_saftey_card(employee.get('employeeId'))
                    #print(json.dumps(card, indent=4))
                    print(f"Card: {card}")
                else:
                    continue """
                
                """ kompetansarray = []

                if( harlastebil ):
                    kompetansarray.append('Lastebil')
                if( hardrone ):
                    kompetansarray.append('Drone')
                if( harstore_aggregat ):
                    kompetansarray.append('Store aggregat')
                if( harsafty_cut ):
                    kompetansarray.append('Safety cut')
                if( len(kompetansarray) > 0 ):
                    courses = ', '.join(kompetansarray) """
                #shortname, description = get_saftey_card(employee.get('employeeId'))

                upsert_driver(cursor, employee, courses )
                conn.commit()
                #print(f"Updated driver with ID: {employee.get('email')}")
                #print(f" Name: {employee.get('email')}, ")
            elif employee.get('mobilePhone') and employee.get('hiredCompanyName') == 'EVINY SOLUTIONS AS':
                print(f"Processing EVINY SOLUTIONS AS: {employee.get('mobilePhone')}, ID: {employee.get('employeeId')}")
                courses = ""
                driver_id = employee.get('employeeId')

                kompetansarray = []

                for course in allcourses:
                    if any(entry["employeeId"] == driver_id for entry in course.get(list(course.keys())[0], [])):
                        kompetansarray.append(list(course.keys())[0])
                if( len(kompetansarray) > 0 ):
                    courses = ', '.join(kompetansarray)
                    print(f"Courses found for employee: {kompetansarray}, ID: {driver_id}")
                #shortname, description = get_saftey_card(employee.get('employeeId'))

                upsert_driver(cursor, employee, courses )
                conn.commit()
                #print(f"Updated driver with ID: {employee.get('email')}")
                #print(f" Name: {employee.get('email')}, ")
            elif employee.get('mobilePhone') and employee.get('companyName') == 'Nordvest Nett AS':
                print(f"Processing Nordvest Nett AS: {employee.get('mobilePhone')}, ID: {employee.get('employeeId')}")
                courses = ""
                driver_id = employee.get('employeeId')

                kompetansarray = []
                for course in allcourses:
                    if any(entry["employeeId"] == driver_id for entry in course.get(list(course.keys())[0], [])):
                        kompetansarray.append(list(course.keys())[0])
                if( len(kompetansarray) > 0 ):
                    courses = ', '.join(kompetansarray)
                    print(f"Courses found for employee: {kompetansarray}, ID: {driver_id}")
                #shortname, description = get_saftey_card(employee.get('employeeId'))

                upsert_driver(cursor, employee, courses )
                conn.commit()
                #print(f"Updated driver with ID: {employee.get('email')}")
                #print(f" Name: {employee.get('email')}, ")
    else:
        print("No employees found or an error occurred.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
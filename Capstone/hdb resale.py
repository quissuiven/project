import json
import requests
import pandas as pd
import pprint
import pyodbc
import datetime

url = 'https://data.gov.sg/api/action/datastore_search?resource_id=1b702208-44bf-4829-b620-4615ee19b57c'

for i in range(500):
    response = requests.get(url)
	data = json.loads(response.text)
    if data['result']['offset'] <= data['result']['total']:
        
        result = data['result']['records']

        town = []
        flat_type = []
        flat_model = []
        floor_area_sqm = []
        street_name = []
        resale_price = []
        month = []
        remaining_lease = []
        lease_commence_date = []
        storey_range = []
        _id = []
        block = []

        for index,row in enumerate(result):
            for keys,values in result[index].items():
                if keys=='town':
                    town.append(values)
                elif keys=='flat_type':
                    flat_type.append(values)
                elif keys=='flat_model':
                    flat_model.append(values)
                elif keys=='floor_area_sqm':
                    floor_area_sqm.append(values)
                elif keys=='street_name':
                    street_name.append(values.replace("'",""))
                elif keys=='resale_price':
                    resale_price.append(values)
                elif keys=='month':
                    month.append(values)
                elif keys=='remaining_lease':
                    remaining_lease.append(values)
                elif keys=='lease_commence_date':
                    lease_commence_date.append(values)
                elif keys=='storey_range':
                    storey_range.append(values)
                elif keys=='_id':
                    _id.append(values)
                elif keys=='block':
                    block.append(values)


        hdb = list(zip(town, flat_type, flat_model, floor_area_sqm, street_name, resale_price, month, remaining_lease, lease_commence_date, storey_range, _id, block))

        db_host = 'DESKTOP-2A5SVIV\SQLEXPRESS'
        db_name = 'singapore'
        db_user = 'DESKTOP-2A5SVIV\seinchyi'
        db_password = ''

        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=DESKTOP-2A5SVIV\SQLEXPRESS; DATABASE=singapore; trusted_Connection=yes'
        db = pyodbc.connect(connection_string)
        print("Connected")
        cursor = db.cursor()

        for value in hdb: 
            SQLCommand = ("INSERT INTO hdb VALUES {}".format(value))
            cursor.execute(SQLCommand)
        db.commit()
        #closing connection    
        print(i, data['result']['offset'])
        print("Data Successfully Inserted")   
        db.close()

        next_url = data['result']['_links']['next']
        url = 'https://data.gov.sg/{}'.format(next_url)
        i+=1
    else:
        break
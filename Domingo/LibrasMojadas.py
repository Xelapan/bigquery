# %%
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import os
from pandas_gbq import to_gbq
from google.cloud import bigquery
import utils.bdds as bdds

current_directory = os.getcwd()
print(current_directory)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = bdds.GetGCP()

# %%
# Test 
#Probar si funciona la funcion
#loaded_data_produccion = getMysqlData(date_value)
#loaded_data_produccion

# %%
# Test 
# Por si se necesita guardar localmente
#save to csv file
#loaded_data_produccion.to_csv(current_directory + "\\file.csv", index=False)

# %%
# Specify your BigQuery project_id, dataset_id, and table_id
project_id = 'digital-bonfire-344816'
dataset_id = 'Produccion'
table_id = 'ResumenProduccion'

# Initialize a BigQuery client
client = bigquery.Client()

# %%
#Borrar los ultimos 60 dias de informacion de BigQUery
sql_delete = f"""DELETE FROM {project_id}.{dataset_id}.{table_id} 
        WHERE
        fechaproduccion >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY);"""

# Execute the query
job = client.query(sql_delete)

# Wait for the query to complete
job.result()

print(f"Deleted {job.num_dml_affected_rows} rows.")

# %%
# Obtener el ultimo dia con informacion en BigQuery
# SQL query to retrieve MAX(date) to update data from that date
sql_query = f"""
    SELECT MAX(fechaproduccion) as LastDate FROM `digital-bonfire-344816.Produccion.ResumenProduccion` 
LIMIT 100"""
client = bigquery.Client()

# Execute the query
query_job = client.query(sql_query)
results = query_job.result()

# Extract the date value from the result
for row in results:
    date_value = row.LastDate

print(date_value.strftime('%Y-%m-%d'))

# %%
loaded_data_produccion = None
#loaded_data_produccion = getMysqlData('2019-12-31')
loaded_data_produccion = bdds.getMysqlData(date_value.strftime('%Y-%m-%d'))

#change fechaprodduccion to datetime column type
loaded_data_produccion['fechaproduccion'] = pd.to_datetime(loaded_data_produccion['fechaproduccion'])

# %%
loaded_data_produccion.head()

# %%
loaded_data_produccion.tail()

# %%
#Cargar desde un dia despues de la ultima fecha cargada
# Write the DataFrame to a BigQuery table
to_gbq(loaded_data_produccion, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists='append')
print("Loaded data to table {}".format(table_id))



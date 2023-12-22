# %%
import os
from datetime import datetime, timedelta
from google.cloud import storage
import pymysql
import pymssql
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import bigquery

current_directory = os.getcwd()
print(current_directory)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = current_directory + "\\digital-bonfire-344816-51ed21bcc910.json"



# %%
def getMysqlData(fecha):
    host = '172.26.200.11'
    user = 'root'
    password = 'admininformatica'
    database = 'dw'

    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=database,
        charset="utf8mb4",
        )

    # Define the SQL query to select data from a table
    sql_libras = f"""Select 
        fechaproduccion,  year(fechaproduccion) year, month(fechaproduccion) month, 
        case WHEN (WEEK(fechaproduccion,3) >= 52 AND DAYOFYEAR(fechaproduccion) <= 3 ) THEN 1 ELSE WEEK(fechaproduccion,3) END as week,
        articulo, turno,
        SUM(libras) libras, SUM(cantidad) unidades_solicitadas, SUM(entregado) unidades_entregadas
          from (
    SELECT
    pro_orden.fechaproduccion,
    pro_orden.articulo,
    pro_orden.codigo as turno,
    0 as libras,
    SUM(pro_orden.cantidad) cantidad,
    SUM(pro_orden.entregado) entregado
    FROM
    dw.pro_orden
    where
    (fechaproduccion) > '{fecha}' 
    and Estado = 'Cerrada'
    group by
    pro_orden.fechaproduccion, pro_orden.articulo,pro_orden.codigo
    UNION ALL
    SELECT
        fechaproduccion,
        producto as articulo,
        turno,
        libras,
        0 as cantidad,
        0 as entregado
    FROM
        dw.pro_librasmojadas
    WHERE
            (`pro_librasmojadas`.`fechaproduccion`) > '{fecha}' 
    ) pro
    group by fechaproduccion, articulo,turno"""

    try:
        # Use Pandas to read data from MySQL into a DataFrame
        result = pd.read_sql(sql_libras, connection)

    except Exception as e:
        print("Error:", str(e))

    finally:
        # Close the database connection when done
        connection.close()
        return result

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
loaded_data_produccion = getMysqlData(date_value.strftime('%Y-%m-%d'))

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



from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import os
from pandas_gbq import to_gbq
from google.cloud import bigquery
import utils.bdds as bdds
from utils.logger import Logger
from utils import sendwp

logger = Logger() 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = bdds.GetGCP()



sendwp.send_message("Iniciando LibrasMojadas.py","52009468")


# Specify global variables
project_id = 'digital-bonfire-344816'
dataset_id = 'XP'
table_id = 'PRO_librasMojadas'
Dias_borrar = 60
Fecha_obtener_Datos = 'fechaproduccion'
schema = 'dw'
try:
    # Initialize a BigQuery client
    client = bigquery.Client()
 
    #
    #
    # Borrar datos en GCP
    #
    #   
    #Borrar los ultimos 60 dias de informacion de BigQUery
    sql_delete = f"""DELETE FROM {project_id}.{dataset_id}.{table_id} 
            WHERE
            {Fecha_obtener_Datos} >= DATE_SUB(CURRENT_DATE(), INTERVAL {Dias_borrar} DAY);"""

    # Execute the query
    job = client.query(sql_delete)

    # Wait for the query to complete
    job.result()

    # Obtener el ultimo dia con informacion en BigQuery
    # SQL query to retrieve MAX(date) to update data from that date
    sql_query = f"""
        SELECT MAX({Fecha_obtener_Datos}) as LastDate FROM {project_id}.{dataset_id}.{table_id} LIMIT 100"""
    client = bigquery.Client()

    # Execute the query
    query_job = client.query(sql_query)
    results = query_job.result()

    # Extract the date value from the result
    for row in results:
        date_value = row.LastDate

    fecha = date_value.strftime('%Y-%m-%d')

    #
    #
    # Cargar datos de BDD local
    #
    #
    qry = f"""Select 
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
    # Load data from BDD
    loaded_data_produccion = None
    loaded_data_produccion = bdds.getMysqlData(qry,schema)

    #change fechaprodduccion to datetime column type
    loaded_data_produccion[Fecha_obtener_Datos] = pd.to_datetime(loaded_data_produccion[Fecha_obtener_Datos])

    #
    #
    # Cargar datos a GCP
    #
    #
    #Cargar desde un dia despues de la ultima fecha cargada
    # Write the DataFrame to a BigQuery table
    to_gbq(loaded_data_produccion, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists='append')
    logger.log("Loaded data to table {}".format(table_id))
except Exception as e:
    logger.log(f"Excepción en LibrasMojadas: {e}")
    sendwp.send_message(f"Excepción en LibrasMojadas.py: {e}","52009468")


import pymysql
import pymssql
import os
import pandas as pd
import json

def GetGCP():
    current_directory = os.getcwd()
    return current_directory + "\\digital-bonfire-344816-9c1df57c7578.json"

def getMysqlData(fecha):
    # Connect to the database from json
    # Open and load the JSON file
    with open('digital-bdds.json', 'r') as json_file:
        data = json.load(json_file)

    connection = pymysql.connect(
        host=data['databases'][0]['connection_info']['host'],
        user=data['databases'][0]['connection_info']['user'],
        password=data['databases'][0]['connection_info']['password'],
        db='dw',
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
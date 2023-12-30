import os
import datetime
import subprocess
import time
from utils.logger import Logger

logger = Logger()    

def ejecutar_script_carpeta(nombre_carpeta):
    try:
        path_carpeta = os.path.join(os.getcwd(), nombre_carpeta)

        if not os.path.exists(path_carpeta):
            print(f"La carpeta '{path_carpeta}' no existe en el directorio actual.")
            return

        for archivo in os.listdir(path_carpeta):
            if archivo.endswith('.py'):
                ruta_completa = os.path.join(path_carpeta, archivo)
                print(f"Ejecutando {archivo}...")
                subprocess.run(['python', ruta_completa])
                time.sleep(120)
    except Exception as e:
        print(f"Error: {e}")
        logger.log(f"Excepción en ejecutar script carpeta (base): {e}")

def ejecutar_script_carpeta_desatendido(nombre_carpeta):
    try:
        path_carpeta = os.path.join(os.getcwd(), nombre_carpeta)

        if not os.path.exists(path_carpeta):
            print(f"La carpeta '{path_carpeta}' no existe en el directorio actual.") # Esto escribirlo en log
            return

        for archivo in os.listdir(path_carpeta):
            if archivo.endswith('.py'):
                ruta_completa = os.path.join(path_carpeta, archivo)
                subprocess.Popen(['python', ruta_completa])
              #  print("Estoy en funcion ejecutar_script_carpeta_desatendido")
                time.sleep(120)
    except Exception as e:
        print(f"Error: {e}")
        logger.log(f"Excepción en ejecutar carpeta destendido (base): {e}")

while True:
    hoy = datetime.datetime.today()
    hora_actual = hoy.hour
    dia_semana = hoy.weekday()  # 0 = Lunes, 1 = Martes, ..., 6 = Domingo
    try:
        if dia_semana == 6:  # Domingo
          #  print("Es domingo")
            ejecutar_script_carpeta('Domingo')

        if 22 <= hora_actual < 24:  # Entre las 10 PM y la medianoche
         #   print("Es entre las 10 PM y la medianoche")
            ejecutar_script_carpeta('Diario')

        if 8 <= hora_actual < 17:  # Entre las 8 AM y las 5 PM
         #   print("Recurrente")
            ejecutar_script_carpeta_desatendido('Recurrente')
    except Exception as e:
        print(f"Error: {e}")
        logger.log(f"Excepción en inicio (base): {e}")
    finally:
        if dia_semana == 6:  # Domingo
            time.sleep((60*60)*23) # dormir 60 segundos *60 min *23 horas = 2 horas
        time.sleep(60*120) # dormir 60 segundos *120 min = 2 horas

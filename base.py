import os
import datetime
import subprocess


class Base:

    def __init__(self):
        hoy = datetime.datetime.today()
        self.hora_actual = hoy.hour
        self.dia_semana = hoy.weekday()  # 0 = Lunes, 1 = Martes, ..., 6 = Domingo

    def ejecutar_script_carpeta(self, nombre_carpeta):
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
        except Exception as e:
            print(f"Error: {e}")


    def ejecutar_script_carpeta_desatendido(self, nombre_carpeta):
        try:
            path_carpeta = os.path.join(os.getcwd(), nombre_carpeta)

            if not os.path.exists(path_carpeta):
                print(f"La carpeta '{path_carpeta}' no existe en el directorio actual.")
                return

            for archivo in os.listdir(path_carpeta):
                if archivo.endswith('.py'):
                    ruta_completa = os.path.join(path_carpeta, archivo)
                    subprocess.Popen(['python', ruta_completa])
        except Exception as e:
            print(f"Error: {e}")

    def inicio(self):
        try:
            if self.dia_semana == 6:  # Domingo
                self.ejecutar_script_carpeta('Domingo')

            if 22 <= self.hora_actual < 24:  # Entre las 10 PM y la medianoche
                self.ejecutar_script_carpeta('Diario')

            if 8 <= self.hora_actual < 17:  # Entre las 8 AM y las 5 PM
                self.ejecutar_script_carpeta_desatendido('Recurrente')
        except Exception as e:
            print(f"Error: {e}")

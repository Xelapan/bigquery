import datetime
import os

class Logger:
    def __init__(self, log_directory="logs"):
        try:
            self.log_directory = log_directory
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
        except Exception as e:
            print(f"Error al inicializar Logger: {e}")

    def log(self, message):
        try:
            # Generar el nombre del archivo de log basado en la fecha actual
            log_filename = datetime.datetime.now().strftime("%Y-%m-%d") + ".log"
            log_filepath = os.path.join(self.log_directory, log_filename)

            # Escribir el mensaje en el archivo de log
            with open(log_filepath, "a") as log_file:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_file.write(f"[{timestamp}] {message}\n")
        except Exception as e:
            print(f"Error al escribir en el archivo de log: {e}")

# Ejemplo de uso
if __name__ == "__main__":
    logger = Logger()
    try:
        # Simular una operación que podría generar una excepción
        1 / 0
    except Exception as e:
        logger.log(f"Excepción capturada: {e}")

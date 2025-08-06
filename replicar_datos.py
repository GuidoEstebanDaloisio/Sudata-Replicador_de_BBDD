import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import traceback
import sys

# Crear carpeta de logs si no existe
log_dir = os.path.join(os.path.dirname(__file__), "Logs-replicacion de datos")
os.makedirs(log_dir, exist_ok=True)

# Crear archivo de log con fecha y hora
ahora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = os.path.join(log_dir, f"log_{ahora}.txt")

# Redirigir salida estándar al archivo de log
class Logger:
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.log = open(filepath, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = sys.stderr = Logger(log_path)

try:
    # Configuración por defecto (modo local)
    usuario_local = "postgres"
    password_local = "PruebaTecnica123;"
    host_local = "localhost"
    puerto_local = "5432"
    bd_local = "replicacion_local"

    usuario_nube = "replica_user"
    password_nube = "yunJPNyKaeGU4ArfvZ38YL760Vp8tHnH"
    host_nube = "dpg-d29ka82li9vc73fr9vi0-a.frankfurt-postgres.render.com"
    puerto_nube = "5432"
    bd_nube = "replicacion_nube"

    # Si existen variables de entorno, usarlas (modo GitHub Actions o entorno cloud)
    usuario_local = os.getenv("PG_USER_LOCAL", usuario_local)
    password_local = os.getenv("PG_PASS_LOCAL", password_local)
    host_local = os.getenv("PG_HOST_LOCAL", host_local)
    puerto_local = os.getenv("PG_PORT_LOCAL", puerto_local)
    bd_local = os.getenv("PG_DB_LOCAL", bd_local)

    usuario_nube = os.getenv("PG_USER_NUBE", usuario_nube)
    password_nube = os.getenv("PG_PASS_NUBE", password_nube)
    host_nube = os.getenv("PG_HOST_NUBE", host_nube)
    puerto_nube = os.getenv("PG_PORT_NUBE", puerto_nube)
    bd_nube = os.getenv("PG_DB_NUBE", bd_nube)

    # Crear conexiones
    engine_local = create_engine(f"postgresql://{usuario_local}:{password_local}@{host_local}:{puerto_local}/{bd_local}")
    engine_nube = create_engine(f"postgresql://{usuario_nube}:{password_nube}@{host_nube}:{puerto_nube}/{bd_nube}")

    # Borra todas las tablas con CASCADE para evitar conflictos con claves foráneas
    with engine_nube.connect() as conn:
        print("\n> Eliminando todas las tablas en la base nube (orden + CASCADE)...")
        conn.execute(text("DROP TABLE IF EXISTS ventas CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS productos CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS customer_segment CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS fechas CASCADE"))
        conn.commit()

    # Listado de tablas y carga
    tablas = ['fechas', 'productos', 'customer_segment', 'ventas']

    for tabla in tablas:
        print(f"\n> Extrayendo datos desde la base local: {tabla}")
        df = pd.read_sql(f"SELECT * FROM {tabla}", engine_local)

        print(f"> Replicando en la base nube: {tabla}")
        df.to_sql(tabla, engine_nube, if_exists='replace', index=False)
        print(f">>> {tabla} replicada correctamente.")

    print("\n>>> ¡Replicación completada con éxito!")

except Exception as e:
    print("\n <<X>> Ocurrió un error durante la replicación de datos:")
    traceback.print_exc()

finally:
    print(f"\n> Log guardado en: {log_path}")

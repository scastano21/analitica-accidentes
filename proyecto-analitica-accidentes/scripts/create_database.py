"""
Script para crear base de datos SQLite del proyecto de Accidentes Viales
Autor: Sebastian Castaño
Fecha: Noviembre 2024
Dataset: Global Road Accidents Dataset (Kaggle)
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Rutas de archivos
DATA_DIR = 'data'
DB_DIR = 'db'
CSV_INPUT = os.path.join(DATA_DIR, 'road_accidents.csv')  # Tu dataset descargado
DB_FILE = os.path.join(DB_DIR, 'proyecto.db')
CSV_EXPORT = os.path.join(DB_DIR, 'export.csv')

# Crear directorios si no existen
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

print("="*70)
print("🚗 PROYECTO: ANÁLISIS DE ACCIDENTES VIALES")
print("="*70)
print(f"\n📅 Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# PASO 1: CARGAR DATASET
# ============================================================================

print("📂 PASO 1: Cargando dataset...")
print("-" * 70)

try:
    # Leer el CSV
    df = pd.read_csv(CSV_INPUT)
    
    print(f"✓ Dataset cargado exitosamente")
    print(f"  - Registros: {len(df):,}")
    print(f"  - Columnas: {len(df.columns)}")
    print(f"  - Tamaño en memoria: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Mostrar primeras filas
    print(f"\n📋 Primeras 3 filas del dataset:")
    print(df.head(3).to_string())
    
    # Información de columnas
    print(f"\n📊 Información de columnas:")
    print(df.info())
    
except FileNotFoundError:
    print(f"❌ ERROR: No se encontró el archivo {CSV_INPUT}")
    print(f"\n💡 INSTRUCCIONES:")
    print(f"   1. Ve a: https://www.kaggle.com/datasets/ankushpanday1/global-road-accidents-dataset")
    print(f"   2. Descarga el dataset (requiere cuenta de Kaggle)")
    print(f"   3. Guarda el archivo CSV en: {DATA_DIR}/road_accidents.csv")
    print(f"   4. Ejecuta este script nuevamente")
    exit(1)

# ============================================================================
# PASO 2: LIMPIEZA DE DATOS
# ============================================================================

print("\n\n🧹 PASO 2: Limpieza de datos...")
print("-" * 70)

# Registrar datos antes de limpiar
original_rows = len(df)

# Eliminar duplicados
df = df.drop_duplicates()
print(f"✓ Duplicados eliminados: {original_rows - len(df)}")

# Eliminar filas con muchos valores nulos (opcional)
df = df.dropna(thresh=len(df.columns) * 0.5)  # Mantener filas con al menos 50% de datos
print(f"✓ Filas con exceso de nulos eliminadas: {original_rows - len(df)}")

# Renombrar columnas a nombres más limpios (ajusta según tu dataset)
# Ejemplo:
column_mapping = {
    'ID': 'id',
    'Severity': 'severity',
    'Start_Time': 'start_time',
    'End_Time': 'end_time',
    'Location': 'location',
    'City': 'city',
    'State': 'state',
    'Zipcode': 'zipcode',
    'Country': 'country',
    'Weather_Condition': 'weather_condition',
    'Temperature(F)': 'temperature_f',
    'Visibility(mi)': 'visibility_mi',
    'Precipitation(in)': 'precipitation_in',
    'Traffic_Signal': 'traffic_signal',
    'Crossing': 'crossing',
    'Junction': 'junction',
    'Stop': 'stop',
    'Railway': 'railway'
}

# Solo renombrar columnas que existen
existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
df = df.rename(columns=existing_columns)

print(f"✓ Columnas renombradas: {len(existing_columns)}")
print(f"✓ Total de registros finales: {len(df):,}")

# ============================================================================
# PASO 3: CREAR BASE DE DATOS SQLITE
# ============================================================================

print("\n\n💾 PASO 3: Creando base de datos SQLite...")
print("-" * 70)

# Eliminar base de datos anterior si existe
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print(f"✓ Base de datos anterior eliminada")

# Conectar a SQLite
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

print(f"✓ Conexión establecida: {DB_FILE}")

# Crear tabla principal
create_table_sql = """
CREATE TABLE IF NOT EXISTS accidents (
    id INTEGER PRIMARY KEY,
    severity INTEGER,
    start_time TEXT,
    end_time TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    country TEXT,
    weather_condition TEXT,
    temperature_f REAL,
    visibility_mi REAL,
    precipitation_in REAL,
    traffic_signal BOOLEAN,
    crossing BOOLEAN,
    junction BOOLEAN,
    stop BOOLEAN,
    railway BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

cursor.execute(create_table_sql)
print(f"✓ Tabla 'accidents' creada")

# ============================================================================
# PASO 4: INSERTAR DATOS
# ============================================================================

print("\n\n📥 PASO 4: Insertando datos en la base de datos...")
print("-" * 70)

# Insertar datos usando pandas (más eficiente)
try:
    # Seleccionar solo las columnas que existen en el DataFrame
    columns_to_insert = [col for col in [
        'id', 'severity', 'start_time', 'end_time', 'location', 'city', 
        'state', 'zipcode', 'country', 'weather_condition', 'temperature_f',
        'visibility_mi', 'precipitation_in', 'traffic_signal', 'crossing',
        'junction', 'stop', 'railway'
    ] if col in df.columns]
    
    # Insertar en la base de datos
    df[columns_to_insert].to_sql('accidents', conn, if_exists='append', index=False)
    
    print(f"✓ {len(df):,} registros insertados exitosamente")
    
    # Verificar inserción
    cursor.execute("SELECT COUNT(*) FROM accidents")
    count = cursor.fetchone()[0]
    print(f"✓ Verificación: {count:,} registros en la base de datos")
    
except Exception as e:
    print(f"❌ Error al insertar datos: {e}")
    conn.close()
    exit(1)

# ============================================================================
# PASO 5: CREAR ÍNDICES PARA OPTIMIZAR CONSULTAS
# ============================================================================

print("\n\n⚡ PASO 5: Creando índices para optimización...")
print("-" * 70)

indexes = [
    "CREATE INDEX IF NOT EXISTS idx_severity ON accidents(severity);",
    "CREATE INDEX IF NOT EXISTS idx_city ON accidents(city);",
    "CREATE INDEX IF NOT EXISTS idx_state ON accidents(state);",
    "CREATE INDEX IF NOT EXISTS idx_weather ON accidents(weather_condition);",
    "CREATE INDEX IF NOT EXISTS idx_date ON accidents(start_time);"
]

for idx_sql in indexes:
    cursor.execute(idx_sql)
    print(f"✓ Índice creado")

print(f"✓ Total de índices creados: {len(indexes)}")

# ============================================================================
# PASO 6: CREAR VISTAS ÚTILES
# ============================================================================

print("\n\n👁️ PASO 6: Creando vistas de análisis...")
print("-" * 70)

# Vista 1: Resumen por ciudad
view1_sql = """
CREATE VIEW IF NOT EXISTS accidents_by_city AS
SELECT 
    city,
    COUNT(*) as total_accidents,
    AVG(severity) as avg_severity,
    COUNT(CASE WHEN severity >= 3 THEN 1 END) as severe_accidents
FROM accidents
WHERE city IS NOT NULL
GROUP BY city
ORDER BY total_accidents DESC;
"""
cursor.execute(view1_sql)
print(f"✓ Vista 'accidents_by_city' creada")

# Vista 2: Accidentes por condición climática
view2_sql = """
CREATE VIEW IF NOT EXISTS accidents_by_weather AS
SELECT 
    weather_condition,
    COUNT(*) as total_accidents,
    AVG(severity) as avg_severity,
    AVG(visibility_mi) as avg_visibility
FROM accidents
WHERE weather_condition IS NOT NULL
GROUP BY weather_condition
ORDER BY total_accidents DESC;
"""
cursor.execute(view2_sql)
print(f"✓ Vista 'accidents_by_weather' creada")

# Confirmar cambios
conn.commit()

# ============================================================================
# PASO 7: CONSULTAS DE EJEMPLO
# ============================================================================

print("\n\n📊 PASO 7: Ejecutando consultas de ejemplo...")
print("-" * 70)

# Consulta 1: Top 10 ciudades con más accidentes
print("\n🏙️ Top 10 ciudades con más accidentes:")
query1 = """
SELECT city, total_accidents, avg_severity, severe_accidents
FROM accidents_by_city
LIMIT 10;
"""
result1 = pd.read_sql_query(query1, conn)
print(result1.to_string(index=False))

# Consulta 2: Accidentes por severidad
print("\n\n📈 Distribución por severidad:")
query2 = """
SELECT 
    severity,
    COUNT(*) as cantidad,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM accidents), 2) as porcentaje
FROM accidents
GROUP BY severity
ORDER BY severity;
"""
result2 = pd.read_sql_query(query2, conn)
print(result2.to_string(index=False))

# Consulta 3: Condiciones climáticas más peligrosas
print("\n\n🌦️ Top 5 condiciones climáticas con mayor severidad promedio:")
query3 = """
SELECT 
    weather_condition,
    total_accidents,
    ROUND(avg_severity, 2) as avg_severity
FROM accidents_by_weather
WHERE total_accidents > 100
ORDER BY avg_severity DESC
LIMIT 5;
"""
result3 = pd.read_sql_query(query3, conn)
print(result3.to_string(index=False))

# ============================================================================
# PASO 8: EXPORTAR A CSV
# ============================================================================

print("\n\n📤 PASO 8: Exportando datos a CSV...")
print("-" * 70)

# Exportar tabla completa
export_query = "SELECT * FROM accidents LIMIT 10000;"  # Limitar para el ejemplo
df_export = pd.read_sql_query(export_query, conn)
df_export.to_csv(CSV_EXPORT, index=False)

print(f"✓ Datos exportados a: {CSV_EXPORT}")
print(f"  - Registros exportados: {len(df_export):,}")
print(f"  - Tamaño del archivo: {os.path.getsize(CSV_EXPORT) / 1024:.2f} KB")

# También exportar las vistas
print("\n📊 Exportando vistas de análisis...")

# Export vista 1
view1_export = pd.read_sql_query("SELECT * FROM accidents_by_city", conn)
view1_export.to_csv(os.path.join(DB_DIR, 'accidents_by_city.csv'), index=False)
print(f"✓ Vista 'accidents_by_city' exportada")

# Export vista 2
view2_export = pd.read_sql_query("SELECT * FROM accidents_by_weather", conn)
view2_export.to_csv(os.path.join(DB_DIR, 'accidents_by_weather.csv'), index=False)
print(f"✓ Vista 'accidents_by_weather' exportada")

# ============================================================================
# PASO 9: ESTADÍSTICAS FINALES
# ============================================================================

print("\n\n📊 ESTADÍSTICAS FINALES")
print("=" * 70)

# Tamaño de la base de datos
db_size = os.path.getsize(DB_FILE) / 1024**2
print(f"💾 Tamaño de la base de datos: {db_size:.2f} MB")

# Número de tablas
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"📋 Tablas creadas: {len(tables)}")
for table in tables:
    print(f"   - {table[0]}")

# Número de vistas
cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
views = cursor.fetchall()
print(f"👁️ Vistas creadas: {len(views)}")
for view in views:
    print(f"   - {view[0]}")

# Número de índices
cursor.execute("SELECT name FROM sqlite_master WHERE type='index';")
indexes = cursor.fetchall()
print(f"⚡ Índices creados: {len(indexes)}")

print(f"\n✅ PROCESO COMPLETADO EXITOSAMENTE")
print(f"📁 Archivos generados:")
print(f"   - Base de datos: {DB_FILE}")
print(f"   - CSV exportado: {CSV_EXPORT}")
print(f"   - CSV ciudades: {os.path.join(DB_DIR, 'accidents_by_city.csv')}")
print(f"   - CSV clima: {os.path.join(DB_DIR, 'accidents_by_weather.csv')}")

# Cerrar conexión
conn.close()
print(f"\n🔒 Conexión a la base de datos cerrada")
print("=" * 70)

# ============================================================================
# INSTRUCCIONES DE USO
# ============================================================================

print("\n\n📖 CÓMO USAR LA BASE DE DATOS:")
print("-" * 70)
print("""
1. DESDE DB BROWSER FOR SQLITE:
   - Descarga: https://sqlitebrowser.org/
   - Abre el archivo: db/proyecto.db
   - Explora las tablas y vistas
   - Ejecuta consultas SQL personalizadas

2. DESDE PYTHON:
   import sqlite3
   conn = sqlite3.connect('db/proyecto.db')
   df = pd.read_sql_query("SELECT * FROM accidents LIMIT 100", conn)
   
3. CONSULTAS ÚTILES:
   - Ver todas las ciudades: SELECT * FROM accidents_by_city;
   - Accidentes severos: SELECT * FROM accidents WHERE severity >= 3;
   - Por clima: SELECT * FROM accidents_by_weather;

4. EXPORTAR MÁS DATOS:
   - Ejecuta consultas en DB Browser
   - File → Export → Table(s) as CSV
""")
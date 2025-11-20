"""
Script CORREGIDO para Etapa 2
Limpieza, enriquecimiento y análisis de accidentes viales
Autor: Sebastian Castaño
Fecha: Noviembre 2024
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
DB_DIR = '../db'
CSV_INPUT = os.path.join(DATA_DIR, 'road_accidents.csv')
DB_FILE = os.path.join(DB_DIR, 'proyecto.db')
CSV_EXPORT = os.path.join(DB_DIR, 'export.csv')
CSV_ENRICHED = os.path.join(DATA_DIR, 'dataset_enriquecido.csv')

# Crear directorios
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

print("=" * 80)
print("🚗 ETAPA 2: LIMPIEZA Y ENRIQUECIMIENTO DE DATOS")
print("=" * 80)
print(f"\n📅 Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# PASO 1: CARGAR DATASET
# ============================================================================

print("📂 PASO 1: Cargando dataset...")
print("-" * 80)

if not os.path.exists(CSV_INPUT):
    print(f"❌ ERROR: No se encontró el archivo {CSV_INPUT}")
    print(f"\n💡 SOLUCIÓN:")
    print(f"   1. Ve a: https://www.kaggle.com/datasets/ankushpanday1/global-road-accidents-dataset")
    print(f"   2. Descarga el dataset (requiere cuenta de Kaggle)")
    print(f"   3. Extrae el CSV y renómbralo a 'road_accidents.csv'")
    print(f"   4. Guárdalo en: {DATA_DIR}/road_accidents.csv")
    print(f"   5. Ejecuta este script nuevamente")
    exit(1)

try:
    # Leer CSV (ajustar según el dataset real)
    print(f"✓ Archivo encontrado: {CSV_INPUT}")
    print(f"⏳ Cargando datos (esto puede tomar varios minutos)...")
    
    # Leer solo las primeras 10,000 filas para el proyecto académico
    df_original = pd.read_csv(CSV_INPUT, nrows=10000, low_memory=False)
    
    print(f"✓ Dataset cargado exitosamente")
    print(f"  - Registros: {len(df_original):,}")
    print(f"  - Columnas: {len(df_original.columns)}")
    print(f"  - Tamaño en memoria: {df_original.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    print(f"\n📋 Columnas encontradas:")
    for i, col in enumerate(df_original.columns, 1):
        print(f"   {i:2d}. {col}")
    
    print(f"\n📋 Primeras 3 filas:")
    print(df_original.head(3))
    
except Exception as e:
    print(f"❌ ERROR al cargar datos: {e}")
    print(f"\n💡 Verifica que el archivo CSV sea válido y esté en el formato correcto")
    exit(1)

# ============================================================================
# PASO 2: LIMPIEZA DE DATOS
# ============================================================================

print("\n\n🧹 PASO 2: LIMPIEZA DE DATOS")
print("=" * 80)

df = df_original.copy()
registros_iniciales = len(df)

# 2.1 Eliminar duplicados
print("\n2.1 ELIMINANDO DUPLICADOS...")
duplicados_antes = df.duplicated().sum()
df = df.drop_duplicates()
duplicados_eliminados = duplicados_antes
print(f"   ✓ Duplicados eliminados: {duplicados_eliminados}")
print(f"   ✓ Registros restantes: {len(df):,}")

# 2.2 Analizar valores nulos
print("\n2.2 ANALIZANDO VALORES NULOS...")
nulos = df.isnull().sum()
nulos_pct = (nulos / len(df)) * 100
columnas_con_nulos = nulos[nulos > 0].sort_values(ascending=False)

if len(columnas_con_nulos) > 0:
    print(f"   ⚠️  Columnas con valores nulos:")
    for col, count in columnas_con_nulos.items():
        pct = (count / len(df)) * 100
        print(f"      - {col}: {count:,} ({pct:.1f}%)")
else:
    print(f"   ✓ No se encontraron valores nulos")

# 2.3 Normalizar nombres de columnas
print("\n2.3 NORMALIZANDO NOMBRES DE COLUMNAS...")
print(f"   Columnas originales (primeras 5): {list(df.columns[:5])}")

columnas_nuevas = {}
for col in df.columns:
    # Convertir a minúsculas
    col_nueva = col.lower().strip()
    # Reemplazar espacios y caracteres especiales
    col_nueva = col_nueva.replace(' ', '_')
    col_nueva = col_nueva.replace('(', '').replace(')', '')
    col_nueva = col_nueva.replace('/', '_')
    col_nueva = col_nueva.replace('-', '_')
    # Quitar acentos
    col_nueva = col_nueva.replace('á', 'a').replace('é', 'e')
    col_nueva = col_nueva.replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
    columnas_nuevas[col] = col_nueva

df = df.rename(columns=columnas_nuevas)
print(f"   ✓ Columnas normalizadas: {len(columnas_nuevas)}")
print(f"   Columnas nuevas (primeras 5): {list(df.columns[:5])}")

# 2.4 Seleccionar columnas relevantes
print("\n2.4 SELECCIONANDO COLUMNAS RELEVANTES...")

# Mapeo de nombres comunes en datasets de accidentes
columnas_comunes = {
    'id': ['id', 'accident_id', 'index'],
    'severity': ['severity', 'accident_severity'],
    'start_time': ['start_time', 'date', 'datetime', 'accident_date'],
    'city': ['city', 'ciudad'],
    'state': ['state', 'estado', 'province'],
    'weather_condition': ['weather_condition', 'weather', 'clima'],
    'temperature_f': ['temperature_f', 'temperature', 'temp'],
    'visibility_mi': ['visibility_mi', 'visibility'],
    'description': ['description', 'descripcion']
}

# Detectar columnas disponibles
columnas_encontradas = {}
for col_estandar, posibles_nombres in columnas_comunes.items():
    for nombre in posibles_nombres:
        if nombre in df.columns:
            columnas_encontradas[col_estandar] = nombre
            break

print(f"   ✓ Columnas mapeadas:")
for estandar, real in columnas_encontradas.items():
    print(f"      {estandar} ← {real}")

# Renombrar a nombres estándar
df = df.rename(columns={v: k for k, v in columnas_encontradas.items()})

# Asegurar que tengamos las columnas mínimas necesarias
columnas_necesarias = ['severity', 'start_time', 'city', 'weather_condition', 
                       'temperature_f', 'visibility_mi']

columnas_faltantes = [col for col in columnas_necesarias if col not in df.columns]

if columnas_faltantes:
    print(f"   ⚠️  Columnas faltantes: {columnas_faltantes}")
    print(f"   Creando columnas sintéticas para demostración...")
    
    # Crear columnas sintéticas si faltan
    if 'severity' not in df.columns:
        df['severity'] = np.random.choice([1, 2, 3, 4], size=len(df), p=[0.4, 0.35, 0.2, 0.05])
    
    if 'city' not in df.columns:
        ciudades = ['Medellín', 'Bogotá', 'Cali', 'Barranquilla', 'Cartagena']
        df['city'] = np.random.choice(ciudades, size=len(df))
    
    if 'weather_condition' not in df.columns:
        climas = ['Despejado', 'Lluvia leve', 'Nublado', 'Lluvia fuerte', 'Neblina']
        df['weather_condition'] = np.random.choice(climas, size=len(df))
    
    if 'temperature_f' not in df.columns:
        df['temperature_f'] = np.random.uniform(50, 95, size=len(df))
    
    if 'visibility_mi' not in df.columns:
        df['visibility_mi'] = np.random.uniform(0.5, 10, size=len(df))

# 2.5 Validar y convertir tipos de datos
print("\n2.5 VALIDANDO TIPOS DE DATOS...")

# Severity
if 'severity' in df.columns:
    df['severity'] = pd.to_numeric(df['severity'], errors='coerce')
    df['severity'] = df['severity'].fillna(2).astype(int)
    df['severity'] = df['severity'].clip(1, 4)
    print(f"   ✓ 'severity' → integer (rango: {df['severity'].min()}-{df['severity'].max()})")

# Temperature
if 'temperature_f' in df.columns:
    df['temperature_f'] = pd.to_numeric(df['temperature_f'], errors='coerce')
    df['temperature_f'] = df['temperature_f'].fillna(df['temperature_f'].median())
    print(f"   ✓ 'temperature_f' → float")

# Visibility
if 'visibility_mi' in df.columns:
    df['visibility_mi'] = pd.to_numeric(df['visibility_mi'], errors='coerce')
    df['visibility_mi'] = df['visibility_mi'].fillna(df['visibility_mi'].median())
    print(f"   ✓ 'visibility_mi' → float")

# Columnas de texto
for col in ['city', 'state', 'weather_condition']:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()
        df[col] = df[col].replace('Nan', 'Desconocido')
        print(f"   ✓ '{col}' → string limpio")

# Start_time
if 'start_time' in df.columns:
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    # Si hay muchos nulos, generar fechas aleatorias
    nulos_fecha = df['start_time'].isnull().sum()
    if nulos_fecha > len(df) * 0.5:
        print(f"   ⚠️  Generando fechas aleatorias ({nulos_fecha} nulos)")
        fecha_inicio = datetime(2023, 1, 1)
        fecha_fin = datetime(2024, 12, 31)
        dias_dif = (fecha_fin - fecha_inicio).days
        
        fechas = []
        for _ in range(len(df)):
            dias_random = random.randint(0, dias_dif)
            fecha = fecha_inicio + timedelta(days=dias_random)
            hora = random.randint(0, 23)
            minuto = random.randint(0, 59)
            fecha = fecha.replace(hour=hora, minute=minuto)
            fechas.append(fecha)
        
        df['start_time'] = fechas
    print(f"   ✓ 'start_time' → datetime")

# 2.6 Resumen de limpieza
print("\n✅ RESUMEN DE LIMPIEZA:")
print(f"   Registros iniciales: {registros_iniciales:,}")
print(f"   Registros finales: {len(df):,}")
print(f"   Duplicados eliminados: {duplicados_eliminados}")
print(f"   Columnas procesadas: {len(df.columns)}")

# ============================================================================
# PASO 3: ENRIQUECIMIENTO DE DATOS
# ============================================================================

print("\n\n📅 PASO 3: ENRIQUECIMIENTO DE DATOS")
print("=" * 80)

# 3.1 Variables temporales
print("\n3.1 CREANDO VARIABLES TEMPORALES...")

if 'start_time' in df.columns:
    df['fecha'] = df['start_time'].dt.date
    df['anio'] = df['start_time'].dt.year
    df['mes'] = df['start_time'].dt.month
    df['mes_nombre'] = df['start_time'].dt.month_name()
    df['dia'] = df['start_time'].dt.day
    df['dia_semana'] = df['start_time'].dt.day_name()
    df['hora'] = df['start_time'].dt.hour
    df['trimestre'] = df['start_time'].dt.quarter
    
    print(f"   ✓ fecha (rango: {df['fecha'].min()} a {df['fecha'].max()})")
    print(f"   ✓ anio (valores: {sorted(df['anio'].unique())})")
    print(f"   ✓ mes (rango: 1-12)")
    print(f"   ✓ mes_nombre")
    print(f"   ✓ dia (rango: 1-31)")
    print(f"   ✓ dia_semana")
    print(f"   ✓ hora (rango: 0-23)")
    print(f"   ✓ trimestre (rango: 1-4)")

# 3.2 Variables categóricas derivadas
print("\n3.2 CREANDO VARIABLES CATEGÓRICAS...")

# Categoría de visibilidad
if 'visibility_mi' in df.columns:
    df['categoria_visibilidad'] = pd.cut(
        df['visibility_mi'],
        bins=[0, 2, 5, 10, float('inf')],
        labels=['Muy Baja (0-2 mi)', 'Baja (2-5 mi)', 'Media (5-10 mi)', 'Alta (>10 mi)']
    )
    print(f"   ✓ categoria_visibilidad")

# Categoría de temperatura
if 'temperature_f' in df.columns:
    df['categoria_temperatura'] = pd.cut(
        df['temperature_f'],
        bins=[0, 40, 60, 80, float('inf')],
        labels=['Fría (<40°F)', 'Templada (40-60°F)', 'Cálida (60-80°F)', 'Muy Cálida (>80°F)']
    )
    print(f"   ✓ categoria_temperatura")

print(f"\n✅ DATASET ENRIQUECIDO:")
print(f"   Registros: {len(df):,}")
print(f"   Columnas totales: {len(df.columns)}")
print(f"   Columnas agregadas: {len(df.columns) - len(df_original.columns)}")

# ============================================================================
# PASO 4: EXPORTAR DATASET ENRIQUECIDO
# ============================================================================

print("\n\n💾 PASO 4: EXPORTANDO DATASET ENRIQUECIDO")
print("=" * 80)

df.to_csv(CSV_ENRICHED, index=False)
print(f"\n✓ Dataset enriquecido exportado:")
print(f"   Ruta: {CSV_ENRICHED}")
print(f"   Registros: {len(df):,}")
print(f"   Columnas: {len(df.columns)}")
print(f"   Tamaño: {os.path.getsize(CSV_ENRICHED) / 1024:.2f} KB")

# ============================================================================
# PASO 5: CREAR BASE DE DATOS SQL ITE
# ============================================================================

print("\n\n💾 PASO 5: CREANDO BASE DE DATOS SQLITE")
print("=" * 80)

# Eliminar BD anterior
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Insertar datos
print(f"\n📥 Insertando datos en SQLite...")
df.to_sql('accidents', conn, if_exists='replace', index=False)

# Verificar
cursor.execute("SELECT COUNT(*) FROM accidents")
count = cursor.fetchone()[0]
print(f"✓ {count:,} registros insertados")

# Crear índices
print(f"\n⚡ Creando índices...")
indices = [
    "CREATE INDEX IF NOT EXISTS idx_severity ON accidents(severity);",
    "CREATE INDEX IF NOT EXISTS idx_city ON accidents(city);",
    "CREATE INDEX IF NOT EXISTS idx_weather ON accidents(weather_condition);",
]

for idx in indices:
    cursor.execute(idx)
print(f"✓ {len(indices)} índices creados")

# Crear vistas
print(f"\n👁️  Creando vistas...")

view1 = """
CREATE VIEW IF NOT EXISTS accidents_by_city AS
SELECT 
    city,
    COUNT(*) as total_accidents,
    ROUND(AVG(severity), 2) as avg_severity
FROM accidents
GROUP BY city
ORDER BY total_accidents DESC;
"""
cursor.execute(view1)

view2 = """
CREATE VIEW IF NOT EXISTS accidents_by_weather AS
SELECT 
    weather_condition,
    COUNT(*) as total_accidents,
    ROUND(AVG(severity), 2) as avg_severity
FROM accidents
GROUP BY weather_condition
ORDER BY total_accidents DESC;
"""
cursor.execute(view2)

print(f"✓ 2 vistas creadas")

conn.commit()

# ============================================================================
# PASO 6: EXPORTAR CSVs
# ============================================================================

print("\n\n📤 PASO 6: EXPORTANDO CSVs DESDE LA BASE DE DATOS")
print("=" * 80)

# Export principal
df_export = pd.read_sql_query("SELECT * FROM accidents", conn)
df_export.to_csv(CSV_EXPORT, index=False)
print(f"\n✓ export.csv: {len(df_export):,} registros")

# Export vistas
view1_df = pd.read_sql_query("SELECT * FROM accidents_by_city", conn)
view1_df.to_csv(os.path.join(DB_DIR, 'accidents_by_city.csv'), index=False)
print(f"✓ accidents_by_city.csv: {len(view1_df):,} registros")

view2_df = pd.read_sql_query("SELECT * FROM accidents_by_weather", conn)
view2_df.to_csv(os.path.join(DB_DIR, 'accidents_by_weather.csv'), index=False)
print(f"✓ accidents_by_weather.csv: {len(view2_df):,} registros")

conn.close()

# ============================================================================
# RESUMEN FINAL
# ============================================================================

print("\n\n" + "=" * 80)
print("✅ PROCESO COMPLETADO EXITOSAMENTE")
print("=" * 80)

print(f"\n📊 RESUMEN:")
print(f"   • Registros procesados: {len(df):,}")
print(f"   • Base de datos: {DB_FILE} ({os.path.getsize(DB_FILE) / 1024**2:.2f} MB)")
print(f"   • Dataset enriquecido: {CSV_ENRICHED}")
print(f"   • Archivos CSV generados: 4")

print(f"\n📁 ARCHIVOS GENERADOS:")
print(f"   1. {CSV_ENRICHED}")
print(f"   2. {CSV_EXPORT}")
print(f"   3. {os.path.join(DB_DIR, 'accidents_by_city.csv')}")
print(f"   4. {os.path.join(DB_DIR, 'accidents_by_weather.csv')}")

print(f"\n🎉 ¡TODO LISTO PARA EL ANÁLISIS EDA!")
print(f"📝 Siguiente paso: Ejecutar el notebook 02_enriquecimiento_eda.ipynb")
print("=" * 80)
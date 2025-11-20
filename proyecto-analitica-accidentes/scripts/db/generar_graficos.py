"""
Script para generar todos los gráficos del EDA
Ejecutar: python scripts/generar_graficos.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# -------------------------------------------------------------------
# CONFIGURACIÓN DE RUTAS (ABSOLUTAS)
# -------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
DOCS_DIR = os.path.join(BASE_DIR, '..', 'docs', 'graficos')

# Crear carpeta si no existe
os.makedirs(DOCS_DIR, exist_ok=True)

# Cargar dataset enriquecido
df = pd.read_csv(os.path.join(DATA_DIR, 'dataset_enriquecido.csv'))

# Estilos
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


# Función para guardar gráficos correctamente
def save_plot(filename):
    plt.savefig(os.path.join(DOCS_DIR, filename), dpi=300)
    plt.close()


# -------------------------------------------------------------------
# GRÁFICO 1: Severidad
# -------------------------------------------------------------------

print("Generando gráfico 1...")
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
severity_counts = df['severity'].value_counts().sort_index()
axes[0].bar(severity_counts.index, severity_counts.values, color='steelblue', edgecolor='black')
axes[0].set_title('Distribución de Severidad')
axes[1].pie(severity_counts, labels=[f'Sev {i}' for i in severity_counts.index], autopct='%1.1f%%')
plt.tight_layout()
save_plot('01_severidad.png')
print("✅ 01_severidad.png")


# -------------------------------------------------------------------
# GRÁFICO 2: Clima
# -------------------------------------------------------------------

print("Generando gráfico 2...")
fig, ax = plt.subplots(figsize=(12, 6))
top_weather = df['weather_condition'].value_counts().head(10)
ax.barh(range(len(top_weather)), top_weather.values, color='coral')
ax.set_yticks(range(len(top_weather)))
ax.set_yticklabels(top_weather.index)
ax.set_title('Top 10 Condiciones Climáticas')
ax.invert_yaxis()
plt.tight_layout()
save_plot('02_clima.png')
print("✅ 02_clima.png")


# -------------------------------------------------------------------
# GRÁFICO 3: Visibilidad
# -------------------------------------------------------------------

print("Generando gráfico 3...")
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].hist(df['visibility_mi'].dropna(), bins=50, color='skyblue', edgecolor='black')
axes[0].set_title('Distribución de Visibilidad')
axes[1].boxplot(df['visibility_mi'].dropna(), vert=False)
axes[1].set_title('Boxplot de Visibilidad')
plt.tight_layout()
save_plot('03_visibilidad.png')
print("✅ 03_visibilidad.png")


# -------------------------------------------------------------------
# GRÁFICO 4: Ciudades
# -------------------------------------------------------------------

print("Generando gráfico 4...")
fig, ax = plt.subplots(figsize=(12, 6))
top_cities = df['city'].value_counts().head(15)
ax.barh(range(len(top_cities)), top_cities.values, color='green')
ax.set_yticks(range(len(top_cities)))
ax.set_yticklabels(top_cities.index)
ax.set_title('Top 15 Ciudades')
ax.invert_yaxis()
plt.tight_layout()
save_plot('04_ciudades.png')
print("✅ 04_ciudades.png")


# -------------------------------------------------------------------
# GRÁFICO 5: Temperatura
# -------------------------------------------------------------------

print("Generando gráfico 5...")
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].hist(df['temperature_f'].dropna(), bins=50, color='orange', edgecolor='black')
axes[0].set_title('Distribución de Temperatura')
axes[1].boxplot(df['temperature_f'].dropna(), vert=False)
axes[1].set_title('Boxplot de Temperatura')
plt.tight_layout()
save_plot('05_temperatura.png')
print("✅ 05_temperatura.png")


# -------------------------------------------------------------------
# GRÁFICO 6: Temporal (Años y Meses)
# -------------------------------------------------------------------

print("Generando gráfico 6...")
fig, axes = plt.subplots(2, 1, figsize=(12, 10))

if 'anio' in df.columns:
    anio_counts = df['anio'].value_counts().sort_index()
    axes[0].bar(anio_counts.index, anio_counts.values, color='purple')
    axes[0].set_title('Accidentes por Año')

if 'mes' in df.columns:
    mes_counts = df['mes'].value_counts().sort_index()
    axes[1].plot(mes_counts.index, mes_counts.values, marker='o', color='navy')
    axes[1].set_title('Accidentes por Mes')

plt.tight_layout()
save_plot('06_temporal.png')
print("✅ 06_temporal.png")


# -------------------------------------------------------------------
# FIN
# -------------------------------------------------------------------

print("\n🎉 ¡Todos los gráficos fueron generados correctamente!")
print(f"📁 Carpeta: {DOCS_DIR}")

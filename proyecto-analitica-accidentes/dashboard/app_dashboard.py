"""
Dashboard Interactivo - Análisis de Accidentes Viales
Proyecto: Análisis Predictivo de Accidentes Viales
Autor: Sebastian Castaño Cossio
IU Digital de Antioquia - 2025
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="Dashboard - Accidentes Viales",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .stMetric {
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Título principal
st.markdown('<h1 class="main-header">🚗 Dashboard - Análisis de Accidentes Viales</h1>', unsafe_allow_html=True)

# Cargar datos
@st.cache_data
def load_data():
    try:
        df = pd.read_csv('../data/dataset_enriquecido.csv')
        # Convertir columnas temporales
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Error al cargar datos: {e}")
        return None

df = load_data()

if df is not None:
    # Sidebar - Filtros
    st.sidebar.header("🔍 Filtros")
    st.sidebar.markdown("Selecciona los filtros para personalizar el análisis:")
    
    # Filtro por año
    if 'anio' in df.columns:
        anios_disponibles = sorted(df['anio'].dropna().unique())
        if len(anios_disponibles) > 0:
            anio_seleccionado = st.sidebar.multiselect(
                "📅 Año",
                options=anios_disponibles,
                default=anios_disponibles
            )
            if anio_seleccionado:
                df = df[df['anio'].isin(anio_seleccionado)]
    
    # Filtro por ciudad
    if 'city' in df.columns:
        ciudades_top = df['city'].value_counts().head(10).index.tolist()
        ciudad_seleccionada = st.sidebar.multiselect(
            "🏙️ Ciudad",
            options=ciudades_top,
            default=ciudades_top[:5]
        )
        if ciudad_seleccionada:
            df = df[df['city'].isin(ciudad_seleccionada)]
    
    # Filtro por severidad
    if 'severity' in df.columns:
        severidades = sorted(df['severity'].unique())
        severidad_seleccionada = st.sidebar.multiselect(
            "⚠️ Severidad",
            options=severidades,
            default=severidades
        )
        if severidad_seleccionada:
            df = df[df['severity'].isin(severidad_seleccionada)]
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"📊 Registros filtrados: **{len(df):,}**")
    
    # Métricas principales
    st.header("📊 Métricas Principales")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Accidentes",
            value=f"{len(df):,}",
            delta=None
        )
    
    with col2:
        if 'severity' in df.columns:
            severidad_promedio = df['severity'].mean()
            st.metric(
                label="Severidad Promedio",
                value=f"{severidad_promedio:.2f}",
                delta=None
            )
    
    with col3:
        if 'city' in df.columns:
            ciudades_unicas = df['city'].nunique()
            st.metric(
                label="Ciudades Afectadas",
                value=f"{ciudades_unicas}",
                delta=None
            )
    
    with col4:
        if 'weather_condition' in df.columns:
            clima_mas_comun = df['weather_condition'].mode()[0] if len(df) > 0 else "N/A"
            st.metric(
                label="Clima Más Común",
                value=clima_mas_comun,
                delta=None
            )
    
    st.markdown("---")
    
    # VARIABLE 1: SEVERIDAD
    st.header("📈 Variable 1: Severidad de Accidentes")
    col1, col2 = st.columns(2)
    
    with col1:
        if 'severity' in df.columns:
            severity_counts = df['severity'].value_counts().sort_index()
            fig_severity = px.bar(
                x=severity_counts.index,
                y=severity_counts.values,
                labels={'x': 'Nivel de Severidad', 'y': 'Cantidad de Accidentes'},
                title='Distribución de Severidad',
                color=severity_counts.values,
                color_continuous_scale='Blues'
            )
            fig_severity.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_severity, use_container_width=True)
    
    with col2:
        if 'severity' in df.columns:
            fig_pie = px.pie(
                values=severity_counts.values,
                names=[f'Severidad {i}' for i in severity_counts.index],
                title='Porcentaje por Severidad',
                hole=0.4
            )
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)
    
    st.markdown("---")
    
    # VARIABLE 2: CONDICIONES CLIMÁTICAS
    st.header("🌦️ Variable 2: Condiciones Climáticas")
    
    if 'weather_condition' in df.columns:
        top_weather = df['weather_condition'].value_counts().head(10)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig_weather = px.bar(
                x=top_weather.values,
                y=top_weather.index,
                orientation='h',
                labels={'x': 'Cantidad de Accidentes', 'y': 'Condición Climática'},
                title='Top 10 Condiciones Climáticas',
                color=top_weather.values,
                color_continuous_scale='Reds'
            )
            fig_weather.update_layout(showlegend=False, height=500)
            st.plotly_chart(fig_weather, use_container_width=True)
        
        with col2:
            st.markdown("### 📊 Datos Clave")
            st.markdown(f"**Total de condiciones:** {df['weather_condition'].nunique()}")
            st.markdown(f"**Más frecuente:** {top_weather.index[0]}")
            st.markdown(f"**Accidentes:** {top_weather.values[0]:,}")
            
            if 'severity' in df.columns:
                weather_severity = df.groupby('weather_condition')['severity'].mean().sort_values(ascending=False).head(5)
                st.markdown("### ⚠️ Mayor Severidad")
                for weather, sev in weather_severity.items():
                    st.markdown(f"- **{weather}**: {sev:.2f}")
    
    st.markdown("---")
    
    # VARIABLE 3: VISIBILIDAD
    st.header("👁️ Variable 3: Visibilidad")
    
    if 'visibility_mi' in df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_hist = px.histogram(
                df,
                x='visibility_mi',
                nbins=50,
                title='Distribución de Visibilidad',
                labels={'visibility_mi': 'Visibilidad (millas)', 'count': 'Frecuencia'},
                color_discrete_sequence=['#17becf']
            )
            fig_hist.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            fig_box = px.box(
                df,
                y='visibility_mi',
                title='Boxplot de Visibilidad',
                labels={'visibility_mi': 'Visibilidad (millas)'},
                color_discrete_sequence=['#17becf']
            )
            fig_box.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_box, use_container_width=True)
        
        # Estadísticas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Media", f"{df['visibility_mi'].mean():.2f} mi")
        with col2:
            st.metric("Mediana", f"{df['visibility_mi'].median():.2f} mi")
        with col3:
            st.metric("Mínimo", f"{df['visibility_mi'].min():.2f} mi")
        with col4:
            st.metric("Máximo", f"{df['visibility_mi'].max():.2f} mi")
    
    st.markdown("---")
    
    # VARIABLE 4: CIUDADES
    st.header("🏙️ Variable 4: Distribución Geográfica")
    
    if 'city' in df.columns:
        top_cities = df['city'].value_counts().head(15)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig_cities = px.bar(
                x=top_cities.values,
                y=top_cities.index,
                orientation='h',
                title='Top 15 Ciudades con Más Accidentes',
                labels={'x': 'Cantidad de Accidentes', 'y': 'Ciudad'},
                color=top_cities.values,
                color_continuous_scale='Greens'
            )
            fig_cities.update_layout(showlegend=False, height=600)
            st.plotly_chart(fig_cities, use_container_width=True)
        
        with col2:
            st.markdown("### 📍 Concentración Geográfica")
            total_accidentes = len(df)
            top_5_sum = top_cities.head(5).sum()
            concentracion = (top_5_sum / total_accidentes) * 100
            
            st.metric("Top 5 Ciudades", f"{concentracion:.1f}%", "del total")
            
            st.markdown("### 🏆 Top 5")
            for i, (ciudad, count) in enumerate(top_cities.head(5).items(), 1):
                porcentaje = (count / total_accidentes) * 100
                st.markdown(f"{i}. **{ciudad}**: {count:,} ({porcentaje:.1f}%)")
    
    st.markdown("---")
    
    # VARIABLE 5: TEMPERATURA
    st.header("🌡️ Variable 5: Temperatura")
    
    if 'temperature_f' in df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_temp_hist = px.histogram(
                df,
                x='temperature_f',
                nbins=50,
                title='Distribución de Temperatura',
                labels={'temperature_f': 'Temperatura (°F)', 'count': 'Frecuencia'},
                color_discrete_sequence=['#ff7f0e']
            )
            fig_temp_hist.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_temp_hist, use_container_width=True)
        
        with col2:
            fig_temp_box = px.box(
                df,
                y='temperature_f',
                title='Boxplot de Temperatura',
                labels={'temperature_f': 'Temperatura (°F)'},
                color_discrete_sequence=['#ff7f0e']
            )
            fig_temp_box.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_temp_box, use_container_width=True)
        
        # Conversión a Celsius
        temp_c_media = (df['temperature_f'].mean() - 32) * 5/9
        temp_c_max = (df['temperature_f'].max() - 32) * 5/9
        temp_c_min = (df['temperature_f'].min() - 32) * 5/9
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Media", f"{df['temperature_f'].mean():.1f}°F", f"{temp_c_media:.1f}°C")
        with col2:
            st.metric("Mediana", f"{df['temperature_f'].median():.1f}°F")
        with col3:
            st.metric("Mínimo", f"{df['temperature_f'].min():.1f}°F", f"{temp_c_min:.1f}°C")
        with col4:
            st.metric("Máximo", f"{df['temperature_f'].max():.1f}°F", f"{temp_c_max:.1f}°C")
    
    st.markdown("---")
    
    # ANÁLISIS TEMPORAL (si está disponible)
    if 'mes' in df.columns and 'anio' in df.columns:
        st.header("📅 Análisis Temporal")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if 'anio' in df.columns:
                anio_counts = df['anio'].value_counts().sort_index()
                fig_anio = px.bar(
                    x=anio_counts.index,
                    y=anio_counts.values,
                    title='Accidentes por Año',
                    labels={'x': 'Año', 'y': 'Cantidad'},
                    color=anio_counts.values,
                    color_continuous_scale='Purples'
                )
                fig_anio.update_layout(showlegend=False)
                st.plotly_chart(fig_anio, use_container_width=True)
        
        with col2:
            if 'mes' in df.columns:
                mes_counts = df['mes'].value_counts().sort_index()
                meses = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 
                         'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
                fig_mes = px.line(
                    x=[meses[i-1] for i in mes_counts.index],
                    y=mes_counts.values,
                    title='Accidentes por Mes',
                    labels={'x': 'Mes', 'y': 'Cantidad'},
                    markers=True
                )
                fig_mes.update_traces(line_color='#2ca02c', marker=dict(size=10))
                st.plotly_chart(fig_mes, use_container_width=True)
    
    # Pie de página
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 2rem;'>
        <p><strong>Proyecto:</strong> Análisis Predictivo de Accidentes Viales</p>
        <p><strong>Autor:</strong> Sebastian Castaño Cossio | IU Digital de Antioquia</p>
        <p><strong>Fecha:</strong> Noviembre 2025</p>
        <p>📊 Dataset: 10,000 registros | 27 variables | Período: 2023-2024</p>
    </div>
    """, unsafe_allow_html=True)

else:
    st.error("❌ No se pudo cargar el dataset. Verifica que el archivo existe en: `data/dataset_enriquecido.csv`")
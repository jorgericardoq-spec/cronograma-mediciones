"""
app_cronograma.py
Visualizador del Cronograma de Mediciones de Pozos
Ejecutar: streamlit run app_cronograma.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import numpy as np

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Cronograma de Mediciones",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Forzar tema claro
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background: #f5f7fa; }
    [data-testid="stHeader"] { background: #f5f7fa; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# ESTILOS
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Mono', monospace;
    background: #f5f7fa;
}

/* Header principal */
.main-header {
    background: linear-gradient(135deg, #0d5c8a 0%, #1178b5 50%, #0a4f7a 100%);
    border: none;
    border-radius: 12px;
    padding: 28px 36px;
    margin-bottom: 28px;
    position: relative;
    overflow: hidden;
    box-shadow: 0 4px 20px rgba(13,92,138,0.25);
}
.main-header::before {
    content: '';
    position: absolute;
    top: -60px; right: -60px;
    width: 220px; height: 220px;
    background: radial-gradient(circle, rgba(255,255,255,0.12) 0%, transparent 70%);
    pointer-events: none;
}
.main-header h1 {
    font-family: 'Syne', sans-serif;
    font-size: 2rem;
    font-weight: 800;
    color: #ffffff;
    margin: 0 0 4px 0;
    letter-spacing: -0.5px;
}
.main-header p {
    color: rgba(255,255,255,0.75);
    font-size: 0.8rem;
    margin: 0;
    letter-spacing: 2px;
    text-transform: uppercase;
}

/* Métricas */
.metric-card {
    background: #ffffff;
    border: 1px solid #dce6f0;
    border-radius: 10px;
    padding: 18px 22px;
    text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.metric-card .value {
    font-family: 'Syne', sans-serif;
    font-size: 2.2rem;
    font-weight: 800;
    color: #0d5c8a;
    line-height: 1;
}
.metric-card .label {
    font-size: 0.72rem;
    color: #4a7fa0;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    margin-top: 6px;
}
.metric-card .sub {
    font-size: 0.78rem;
    color: #7aa8c0;
    margin-top: 4px;
}

/* Sección título */
.section-title {
    font-family: 'Syne', sans-serif;
    font-size: 1rem;
    font-weight: 700;
    color: #0d5c8a;
    letter-spacing: 2px;
    text-transform: uppercase;
    border-left: 3px solid #1178b5;
    padding-left: 12px;
    margin: 24px 0 16px 0;
}

/* Alerta salto forzado */
.alert-salto {
    background: rgba(231,111,81,0.08);
    border: 1px solid rgba(231,111,81,0.5);
    border-radius: 6px;
    padding: 10px 16px;
    font-size: 0.8rem;
    color: #c85a35;
    margin: 8px 0;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #ffffff;
    border-right: 1px solid #dce6f0;
}
section[data-testid="stSidebar"] * {
    color: #1a3a50 !important;
}

/* Dataframe */
.stDataFrame {
    border: 1px solid #dce6f0 !important;
    border-radius: 8px !important;
}

/* Footer */
.footer {
    text-align: center;
    color: #7aa8c0;
    font-size: 0.72rem;
    letter-spacing: 1px;
    margin-top: 40px;
    padding-top: 20px;
    border-top: 1px solid #dce6f0;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# COLORES POR CUADRILLA
# ─────────────────────────────────────────────────────────────
COLOR_A    = "#0077b6"
COLOR_B    = "#e76f51"
COLOR_GRID = "#d0dde8"

# ─────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────────────────────
PARTE53_URL = (
    "https://cpetroleum-my.sharepoint.com/:x:/g/personal/jquintero_clear_com_ar"
    "/IQBcMbRs22J0QJoTqEr5S1jQAdy3bznzcjiKVjPniZXQwxY?e=x4wkqj&download=1"
)

def _parsear_parte53(source) -> pd.DataFrame:
    """Parsea el PARTE 53 desde un path o buffer y devuelve observaciones por pozo."""
    try:
        df = pd.read_excel(source, sheet_name="PARTE NOVEDADES")
        df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
        df["POZO"]  = df["POZO"].astype(str).str.strip().str.upper()
        df = df.sort_values("FECHA", ascending=False)
        df = df.drop_duplicates(subset=["POZO"], keep="first")
        return df[["POZO", "FECHA", "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR"]].copy()
    except Exception:
        return pd.DataFrame(columns=["POZO", "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR"])

@st.cache_data(ttl=300)
def cargar_parte53_sharepoint() -> pd.DataFrame:
    """Descarga el PARTE 53 desde SharePoint (funciona para cualquier usuario)."""
    try:
        import requests, io
        resp = requests.get(PARTE53_URL, timeout=15)
        resp.raise_for_status()
        return _parsear_parte53(io.BytesIO(resp.content))
    except Exception as e:
        return pd.DataFrame(columns=["POZO", "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR"])

@st.cache_data(ttl=60)
def cargar_parte53(path: str) -> pd.DataFrame:
    """Lee el PARTE 53 desde una ruta local."""
    return _parsear_parte53(path)


PARTE53_URL = (
    "https://cpetroleum-my.sharepoint.com/:x:/g/personal/jquintero_clear_com_ar"
    "/IQBcMbRs22J0QJoTqEr5S1jQAdy3bznzcjiKVjPniZXQwxY?e=x4wkqj&download=1"
)

def _parsear_parte53(source) -> pd.DataFrame:
    """Parsea el PARTE 53 desde un path o buffer y devuelve observaciones por pozo."""
    try:
        df = pd.read_excel(source, sheet_name="PARTE NOVEDADES")
        df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
        df["POZO"]  = df["POZO"].astype(str).str.strip().str.upper()
        df = df.sort_values("FECHA", ascending=False)
        df = df.drop_duplicates(subset=["POZO"], keep="first")
        return df[["POZO", "FECHA", "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR"]].copy()
    except Exception:
        return pd.DataFrame(columns=["POZO", "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR"])

@st.cache_data(ttl=300)
def cargar_parte53_sharepoint() -> pd.DataFrame:
    """Descarga el PARTE 53 desde SharePoint (funciona para cualquier usuario)."""
    try:
        import requests, io
        resp = requests.get(PARTE53_URL, timeout=15)
        resp.raise_for_status()
        return _parsear_parte53(io.BytesIO(resp.content))
    except Exception as e:
        return pd.DataFrame(columns=["POZO", "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR"])

@st.cache_data(ttl=60)
def cargar_parte53(path: str) -> pd.DataFrame:
    """Lee el PARTE 53 desde una ruta local."""
    return _parsear_parte53(path)


@st.cache_data(ttl=60)
def cargar_excel(path: str):
    xl   = pd.ExcelFile(path)
    data = {}
    for sheet in xl.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
        # Normalizar fechas
        for col in df.columns:
            if "FECHA" in col.upper():
                df[col] = pd.to_datetime(df[col], errors="coerce")
        data[sheet] = df
    return data


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🛢️ Configuración")
    st.markdown("---")

    # Detectar automáticamente el archivo en la misma carpeta que el script
    _dir_script = Path(__file__).parent
    _ruta_auto  = _dir_script / "CRONOGRAMAS_MEDICIONES.xlsx"

    archivo = str(_ruta_auto)

    if not Path(archivo).exists():
        st.error(
            f"No se encontró `CRONOGRAMAS_MEDICIONES.xlsx` en:\n`{_dir_script}`\n\n"
            "Asegurate de ejecutar la automatización (`python main.py`) al menos una vez."
        )
        st.stop()

    datos = cargar_excel(archivo)

    if st.button("🔄 Recargar si hay un nuevo cronograma", use_container_width=True,
                  help="Hacé clic si generaste un nuevo cronograma y querés ver los datos actualizados"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown("### 📋 PARTE 53")

    # Intentar cargar desde SharePoint (funciona para cualquier usuario de la empresa)
    import os
    _local_parte = next((
        p for p in [
            _dir_script / "PARTE 53- CLEAR.xlsx",
            Path(os.path.expanduser("~")) / "OneDrive - Clear Petroleum SA" / "PARTE 53- CLEAR.xlsx",
        ] if p.exists()
    ), None)

    if _local_parte:
        # Si está disponible localmente (más actualizado), usar ese
        df_parte53 = cargar_parte53(str(_local_parte))
        st.caption(f"✅ {len(df_parte53)} pozos · fuente: OneDrive local")
    else:
        # Descargar desde SharePoint automáticamente
        with st.spinner("Descargando PARTE 53 desde SharePoint..."):
            df_parte53 = cargar_parte53_sharepoint()
        if not df_parte53.empty:
            st.caption(f"✅ {len(df_parte53)} pozos · fuente: SharePoint")
        else:
            st.caption("⚠️ No se pudo descargar — subí el archivo manualmente:")
            _parte_upload = st.file_uploader(
                "PARTE 53- CLEAR.xlsx",
                type=["xlsx"],
                key="parte53_upload",
                label_visibility="collapsed"
            )
            if _parte_upload:
                import io as _io2
                df_parte53 = _parsear_parte53(_io2.BytesIO(_parte_upload.read()))
                st.caption(f"✅ {len(df_parte53)} pozos cargados desde archivo subido")

    st.markdown("---")

    # Vista: DIARIO o SEMANAL
    vista = st.radio(
        "Vista",
        ["DIARIO", "SEMANAL"],
        horizontal=True,
    )

    df_vista = datos[vista].copy()

    # Selector de fecha
    fechas_disp = sorted(df_vista["FECHA"].dropna().dt.date.unique())
    fecha_sel   = st.selectbox(
        "Fecha",
        fechas_disp,
        format_func=lambda d: d.strftime("%d/%m/%Y (%A)").replace(
            "Monday","Lunes").replace("Tuesday","Martes").replace(
            "Wednesday","Miércoles").replace("Thursday","Jueves").replace(
            "Friday","Viernes")
    )

    # Selector de cuadrilla
    cuad_opciones = ["Ambas", "A", "B"]
    cuad_sel      = st.radio("Cuadrilla", cuad_opciones, horizontal=True)

    st.markdown("---")
    st.markdown("### 📋 Resumen semana")
    resumen = df_vista.groupby(["FECHA", "CUADRILLA"]).size().reset_index(name="N")
    for _, r in resumen.iterrows():
        badge = "🔵" if r["CUADRILLA"] == "A" else "🟠"
        st.markdown(f"{badge} **{r['FECHA'].strftime('%d/%m')}** · Cuad {r['CUADRILLA']}: **{r['N']}** pozos")


# ─────────────────────────────────────────────────────────────
# FILTRAR DATOS
# ─────────────────────────────────────────────────────────────
df_dia = df_vista[df_vista["FECHA"].dt.date == fecha_sel].copy()
if cuad_sel != "Ambas":
    df_dia = df_dia[df_dia["CUADRILLA"] == cuad_sel].copy()

df_dia = df_dia.reset_index(drop=True)
df_dia.index = df_dia.index + 1  # posición desde 1

fecha_fmt = fecha_sel.strftime("%d de %B de %Y").replace(
    "January","enero").replace("February","febrero").replace(
    "March","marzo").replace("April","abril").replace("May","mayo").replace(
    "June","junio").replace("July","julio").replace("August","agosto").replace(
    "September","septiembre").replace("October","octubre").replace(
    "November","noviembre").replace("December","diciembre")


# ─────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="main-header">
    <h1>🛢️ Cronograma de Mediciones</h1>
    <p>Vista {vista} · {fecha_fmt} · Cuadrilla {cuad_sel}</p>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────────────────────
n_a       = len(df_dia[df_dia["CUADRILLA"] == "A"])
n_b       = len(df_dia[df_dia["CUADRILLA"] == "B"])
n_total   = len(df_dia)
prod_tot  = df_dia["PROD_NETA"].sum()
n_saltos  = int(df_dia["SALTO_FORZADO"].sum()) if "SALTO_FORZADO" in df_dia.columns else 0
n_bats    = df_dia["BATERIA"].nunique()
n_telm    = df_dia["OBSERVACION"].notna().sum() if "OBSERVACION" in df_dia.columns else 0

cols = st.columns(6)
metricas = [
    (n_total,          "Pozos totales",       f"A:{n_a}  B:{n_b}"),
    (n_bats,           "Baterías",            "en ruta"),
    (f"{prod_tot:.0f}","Producción total",    "bbl/día"),
    (f"{prod_tot/n_total:.1f}" if n_total else "—", "Prod. promedio", "bbl/pozo"),
    (n_saltos,         "Saltos forzados",     "> 3 km"),
    (n_telm,           "Con telemetría",      "solo nivel"),
]
for col, (val, label, sub) in zip(cols, metricas):
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="value">{val}</div>
            <div class="label">{label}</div>
            <div class="sub">{sub}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("")


# ─────────────────────────────────────────────────────────────
# MAPA + GRÁFICO PRODUCCIÓN (lado a lado)
# ─────────────────────────────────────────────────────────────
col_mapa, col_prod = st.columns([3, 2])

# ── MAPA ────────────────────────────────────────────────────
with col_mapa:
    st.markdown('<div class="section-title">📍 Mapa de ruta</div>', unsafe_allow_html=True)

    df_geo = df_dia.dropna(subset=["LAT", "LON"]).copy()
    df_geo["_idx"] = range(1, len(df_geo) + 1)

    if df_geo.empty:
        st.warning("No hay coordenadas disponibles para esta selección.")
    else:
        color_map = {"A": COLOR_A, "B": COLOR_B}
        df_geo["color"] = df_geo["CUADRILLA"].map(color_map)
        df_geo["hover"] = df_geo.apply(
            lambda r: f"#{int(r['_idx'])} {r['POZO']}<br>{r['BATERIA']}<br>{r['PROD_NETA']:.1f} bbl/d<br>{r['DIST_KM']:.2f} km al anterior",
            axis=1
        )

        fig_map = go.Figure()

        # Líneas de ruta por cuadrilla
        for cuad, color in [("A", COLOR_A), ("B", COLOR_B)]:
            dg = df_geo[df_geo["CUADRILLA"] == cuad].sort_values("_idx")
            if len(dg) > 1:
                fig_map.add_trace(go.Scattermapbox(
                    lat=dg["LAT"], lon=dg["LON"],
                    mode="lines",
                    line=dict(color=color, width=2),
                    opacity=0.5,
                    name=f"Ruta {cuad}",
                    hoverinfo="skip",
                ))

        # Puntos por cuadrilla
        for cuad, color, symbol in [("A", COLOR_A, "circle"), ("B", COLOR_B, "circle")]:
            dg = df_geo[df_geo["CUADRILLA"] == cuad]
            fig_map.add_trace(go.Scattermapbox(
                lat=dg["LAT"], lon=dg["LON"],
                mode="markers+text",
                marker=dict(size=10, color=color, opacity=0.9),
                text=dg["_idx"].astype(str),
                textfont=dict(size=8, color="white"),
                textposition="top center",
                name=f"Cuadrilla {cuad}",
                customdata=dg[["POZO", "BATERIA", "PROD_NETA", "DIST_KM"]].values,
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "Batería: %{customdata[1]}<br>"
                    "Producción: %{customdata[2]:.1f} bbl/d<br>"
                    "Dist. anterior: %{customdata[3]:.2f} km<br>"
                    "<extra></extra>"
                ),
            ))

        # Saltos forzados
        df_sf = df_geo[df_geo["SALTO_FORZADO"] == True] if "SALTO_FORZADO" in df_geo.columns else pd.DataFrame()
        if not df_sf.empty:
            fig_map.add_trace(go.Scattermapbox(
                lat=df_sf["LAT"], lon=df_sf["LON"],
                mode="markers",
                marker=dict(size=16, color="#e76f51", symbol="circle", opacity=0.5),
                name="Salto forzado",
                hoverinfo="skip",
            ))

        centro_lat = df_geo["LAT"].mean()
        centro_lon = df_geo["LON"].mean()

        fig_map.update_layout(
            mapbox=dict(
                style="open-street-map",
                center=dict(lat=centro_lat, lon=centro_lon),
                zoom=10,
            ),
            margin=dict(l=0, r=0, t=0, b=0),
            height=480,
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(
                bgcolor="rgba(255,255,255,0.92)",
                bordercolor="#dce6f0",
                borderwidth=1,
                font=dict(color="#1a3a50", size=11),
                x=0.01,
                y=0.99,
                xanchor="left",
                yanchor="top",
            ),
        )
        st.plotly_chart(fig_map, use_container_width=True, config={"scrollZoom": True})


# ── GRÁFICOS PRODUCCIÓN ──────────────────────────────────────
with col_prod:
    st.markdown('<div class="section-title">📊 Producción</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Por pozo", "Por batería", "Por cuadrilla"])

    # Prod por pozo (top 20)
    with tab1:
        df_pp = df_dia.sort_values("PROD_NETA", ascending=True).tail(20)
        fig_pp = go.Figure(go.Bar(
            x=df_pp["PROD_NETA"],
            y=df_pp["POZO"],
            orientation="h",
            marker=dict(
                color=df_pp["CUADRILLA"].map({"A": COLOR_A, "B": COLOR_B}),
                opacity=0.85,
            ),
            hovertemplate="<b>%{y}</b><br>%{x:.1f} bbl/d<extra></extra>",
        ))
        fig_pp.update_layout(
            height=420,
            paper_bgcolor="#ffffff",
            plot_bgcolor="#f8fbfd",
            margin=dict(l=0, r=10, t=10, b=0),
            xaxis=dict(gridcolor="#dce6f0", tickfont=dict(color="#4a7fa0", size=10)),
            yaxis=dict(tickfont=dict(color="#1a3a50", size=9)),
            showlegend=False,
        )
        st.plotly_chart(fig_pp, use_container_width=True)

    # Prod por batería
    with tab2:
        df_bat = (
            df_dia.groupby(["BATERIA", "CUADRILLA"])
            .agg(prod=("PROD_NETA", "sum"), n=("POZO", "count"))
            .reset_index()
            .sort_values("prod", ascending=True)
        )
        fig_bat = go.Figure(go.Bar(
            x=df_bat["prod"],
            y=df_bat["BATERIA"],
            orientation="h",
            marker=dict(
                color=df_bat["CUADRILLA"].map({"A": COLOR_A, "B": COLOR_B}),
                opacity=0.85,
            ),
            customdata=df_bat[["n", "CUADRILLA"]].values,
            hovertemplate="<b>%{y}</b><br>%{x:.1f} bbl/d<br>%{customdata[0]} pozos · Cuad %{customdata[1]}<extra></extra>",
        ))
        fig_bat.update_layout(
            height=420,
            paper_bgcolor="#ffffff",
            plot_bgcolor="#f8fbfd",
            margin=dict(l=0, r=10, t=10, b=0),
            xaxis=dict(gridcolor="#dce6f0", tickfont=dict(color="#4a7fa0", size=10)),
            yaxis=dict(tickfont=dict(color="#1a3a50", size=9)),
            showlegend=False,
        )
        st.plotly_chart(fig_bat, use_container_width=True)

    # Prod por cuadrilla
    with tab3:
        df_cuad = df_dia.groupby("CUADRILLA").agg(
            prod=("PROD_NETA", "sum"),
            pozos=("POZO", "count"),
            baterias=("BATERIA", "nunique"),
        ).reset_index()

        fig_cuad = go.Figure()
        for _, r in df_cuad.iterrows():
            color = COLOR_A if r["CUADRILLA"] == "A" else COLOR_B
            fig_cuad.add_trace(go.Bar(
                name=f"Cuadrilla {r['CUADRILLA']}",
                x=[f"Cuadrilla {r['CUADRILLA']}"],
                y=[r["prod"]],
                marker_color=color,
                opacity=0.85,
                text=f"{r['prod']:.0f} bbl/d<br>{r['pozos']} pozos",
                textposition="inside",
                textfont=dict(color="white", size=12),
                hovertemplate=f"Cuadrilla {r['CUADRILLA']}<br>{r['prod']:.1f} bbl/d<br>{r['pozos']} pozos<br>{r['baterias']} baterías<extra></extra>",
            ))
        fig_cuad.update_layout(
            height=420,
            paper_bgcolor="#ffffff",
            plot_bgcolor="#f8fbfd",
            margin=dict(l=0, r=10, t=10, b=0),
            yaxis=dict(gridcolor="#dce6f0", tickfont=dict(color="#4a7fa0", size=10), title="bbl/día", title_font=dict(color="#4a7fa0")),
            xaxis=dict(tickfont=dict(color="#1a3a50", size=12)),
            showlegend=False,
            bargap=0.3,
        )
        st.plotly_chart(fig_cuad, use_container_width=True)


# ─────────────────────────────────────────────────────────────
# TABLA DE POZOS
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">📋 Detalle de pozos</div>', unsafe_allow_html=True)

# Alertas de saltos forzados
saltos_df = df_dia[df_dia["SALTO_FORZADO"] == True] if "SALTO_FORZADO" in df_dia.columns else pd.DataFrame()
if not saltos_df.empty:
    for _, r in saltos_df.iterrows():
        st.markdown(
            f'<div class="alert-salto">⚠️ Salto forzado: <b>{r["POZO"]}</b> ({r["BATERIA"]}) — {r["DIST_KM"]:.1f} km al pozo anterior</div>',
            unsafe_allow_html=True
        )

# Preparar tabla
cols_mostrar = ["CUADRILLA", "POZO", "BATERIA", "PROD_NETA", "DIST_KM", "SALTO_FORZADO", "OBSERVACION"]
cols_mostrar = [c for c in cols_mostrar if c in df_dia.columns]
df_tabla = df_dia[cols_mostrar].copy()
df_tabla.index.name = "#"

# Renombrar para display
rename = {
    "CUADRILLA": "Cuad",
    "POZO": "Pozo",
    "BATERIA": "Batería",
    "PROD_NETA": "Prod (bbl/d)",
    "DIST_KM": "Dist (km)",
    "SALTO_FORZADO": "Salto",
    "OBSERVACION": "Observación",
}
df_tabla = df_tabla.rename(columns=rename)

# Formatear columnas numéricas
if "Prod (bbl/d)" in df_tabla.columns:
    df_tabla["Prod (bbl/d)"] = df_tabla["Prod (bbl/d)"].round(2)
if "Dist (km)" in df_tabla.columns:
    df_tabla["Dist (km)"] = df_tabla["Dist (km)"].round(2)

# Filtro por cuadrilla en tabla
col_filt1, col_filt2, _ = st.columns([1, 1, 4])
with col_filt1:
    cuad_tabla = st.selectbox("Filtrar tabla", ["Todas", "A", "B"], key="tabla_cuad")
with col_filt2:
    solo_saltos = st.checkbox("Solo saltos forzados", key="solo_saltos")

df_tabla_filtrada = df_tabla.copy()
if cuad_tabla != "Todas":
    df_tabla_filtrada = df_tabla_filtrada[df_tabla_filtrada["Cuad"] == cuad_tabla]
if solo_saltos and "Salto" in df_tabla_filtrada.columns:
    df_tabla_filtrada = df_tabla_filtrada[df_tabla_filtrada["Salto"] == True]

st.dataframe(
    df_tabla_filtrada,
    use_container_width=True,
    height=420,
    column_config={
        "Prod (bbl/d)": st.column_config.NumberColumn(format="%.2f"),
        "Dist (km)": st.column_config.NumberColumn(format="%.2f"),
        "Salto": st.column_config.CheckboxColumn(),
        "Cuad": st.column_config.TextColumn(width="small"),
    }
)

# Descarga Excel
import io as _io
def _generar_excel(df):
    buf = _io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=True, sheet_name="Cronograma")
    return buf.getvalue()

nombre_xlsx = f"cronograma_{fecha_sel.strftime('%Y%m%d')}_cuad{cuad_sel.lower().replace(' ','')}.xlsx"
st.download_button(
    label="⬇️ Descargar tabla como Excel",
    data=_generar_excel(df_tabla_filtrada),
    file_name=nombre_xlsx,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)


# ─────────────────────────────────────────────────────────────
# EXCLUIDOS + ESPERA TRACTOR
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">🚫 Pozos excluidos</div>', unsafe_allow_html=True)

tab_sup, tab_et = st.tabs(["Excluidos por superficie", "Espera tractor"])

with tab_sup:
    df_exc = datos.get("EXCLUIDOS_SUPERFICIE", pd.DataFrame())
    if df_exc.empty:
        st.info("Sin excluidos por superficie.")
    else:
        # Filtrar por fecha
        if "FECHA" in df_exc.columns:
            fechas_exc = sorted(df_exc["FECHA"].dropna().dt.date.unique(), reverse=True)
            fecha_exc_sel = st.selectbox("Fecha", fechas_exc, key="fecha_exc",
                                          format_func=lambda d: d.strftime("%d/%m/%Y"))
            df_exc_f = df_exc[df_exc["FECHA"].dt.date == fecha_exc_sel].copy()
        else:
            df_exc_f = df_exc.copy()

        # Ordenar columnas: las más útiles primero
        cols_orden = ["POZO", "BATERIA", "NOVEDADES", "SOLUCIONADO", "FECHA_NOVEDAD", "FECHA"]
        cols_mostrar_exc = [c for c in cols_orden if c in df_exc_f.columns]
        df_exc_f = df_exc_f[cols_mostrar_exc].copy()

        # Traer OBSERVACIONES del PARTE 53 por pozo
        if not df_parte53.empty:
            df_parte53_m = df_parte53.copy()
            df_parte53_m["POZO"] = df_parte53_m["POZO"].astype(str).str.strip().str.upper()
            df_exc_f["_POZO_KEY"] = df_exc_f["POZO"].astype(str).str.strip().str.upper()
            df_exc_f = df_exc_f.merge(
                df_parte53_m[["POZO", "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR"]].rename(
                    columns={"POZO": "_POZO_KEY"}
                ),
                on="_POZO_KEY", how="left"
            ).drop(columns=["_POZO_KEY"])
        else:
            df_exc_f["OBSERVACIONES"] = None
            df_exc_f["OBSERVACIÓN SUPERVISOR"] = None

        # Métricas rápidas
        col_e1, col_e2, col_e3 = st.columns(3)
        n_no_real   = (df_exc_f["NOVEDADES"] == "NO REALIZADO").sum() if "NOVEDADES" in df_exc_f.columns else 0
        n_parcial   = (df_exc_f["NOVEDADES"] == "REALIZADO PARCIAL").sum() if "NOVEDADES" in df_exc_f.columns else 0
        with col_e1:
            st.markdown(f'<div class="metric-card"><div class="value">🔴 {n_no_real}</div><div class="label">No realizados</div></div>', unsafe_allow_html=True)
        with col_e2:
            st.markdown(f'<div class="metric-card"><div class="value">🟡 {n_parcial}</div><div class="label">Realizados parcial</div></div>', unsafe_allow_html=True)
        with col_e3:
            st.markdown(f'<div class="metric-card"><div class="value">{len(df_exc_f)}</div><div class="label">Total excluidos</div></div>', unsafe_allow_html=True)

        st.markdown("")

        # Filtro por tipo de novedad
        if "NOVEDADES" in df_exc_f.columns:
            tipos = ["Todos"] + sorted(df_exc_f["NOVEDADES"].dropna().unique().tolist())
            tipo_sel = st.selectbox("Filtrar por novedad", tipos, key="tipo_novedad")
            if tipo_sel != "Todos":
                df_exc_f = df_exc_f[df_exc_f["NOVEDADES"] == tipo_sel]

        # Reordenar columnas para que observaciones queden visibles
        cols_final = [c for c in [
            "POZO", "BATERIA", "NOVEDADES", "SOLUCIONADO",
            "OBSERVACIONES", "OBSERVACIÓN SUPERVISOR",
            "FECHA_NOVEDAD", "FECHA"
        ] if c in df_exc_f.columns]
        df_exc_f = df_exc_f[cols_final]

        st.dataframe(
            df_exc_f,
            use_container_width=True,
            height=350,
            column_config={
                "FECHA_NOVEDAD":          st.column_config.DateColumn("Fecha novedad", format="DD/MM/YYYY"),
                "FECHA":                  st.column_config.DateColumn("Fecha proceso", format="DD/MM/YYYY"),
                "NOVEDADES":              st.column_config.TextColumn("Novedad", width="medium"),
                "SOLUCIONADO":            st.column_config.TextColumn("¿Solucionado?", width="small"),
                "POZO":                   st.column_config.TextColumn("Pozo", width="small"),
                "BATERIA":                st.column_config.TextColumn("Batería", width="small"),
                "OBSERVACIONES":          st.column_config.TextColumn("Observación operador", width="large"),
                "OBSERVACIÓN SUPERVISOR": st.column_config.TextColumn("Observación supervisor", width="large"),
            }
        )

with tab_et:
    df_et = datos.get("EXCLUIDOS_ESPERA_TRACTOR", pd.DataFrame())
    if df_et.empty:
        st.info("Sin datos de espera tractor.")
    else:
        # Colorear por MOTIVO
        motivo_counts = df_et["MOTIVO"].value_counts() if "MOTIVO" in df_et.columns else pd.Series()

        col_e1, col_e2, col_e3 = st.columns(3)
        for col, motivo, emoji in [
            (col_e1, "EN ESPERA TRACTOR", "🔴"),
            (col_e2, "PARADO POR OTRO MOTIVO", "🟡"),
            (col_e3, "VENTANA CUMPLIDA - PUEDE MEDIRSE", "🟢"),
        ]:
            n = sum(v for k, v in motivo_counts.items() if motivo in k)
            with col:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="value">{emoji} {n}</div>
                    <div class="label" style="font-size:0.65rem">{motivo}</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("")

        # Filtro por motivo
        motivos = ["Todos"] + sorted(df_et["MOTIVO"].unique().tolist()) if "MOTIVO" in df_et.columns else ["Todos"]
        motivo_sel = st.selectbox("Filtrar por motivo", motivos, key="motivo_et")
        df_et_f = df_et if motivo_sel == "Todos" else df_et[df_et["MOTIVO"] == motivo_sel]
        st.dataframe(df_et_f, use_container_width=True, height=300)


# ─────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="footer">
    CRONOGRAMA DE MEDICIONES · CLEAR PETROLEUM · AUTOMATIZACIÓN INFOIL API
</div>
""", unsafe_allow_html=True)

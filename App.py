
import os
from datetime import datetime, date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------
# Configuración/Secrets
# ------------------------- 
MAX_PER_TYPE = {"Comercial": 10, "Industrial": 10}

def get_count_by_type(work_date_str: str, tipo: str) -> int:
    with engine.begin() as conn:
        cnt = conn.execute(
            text("""
                SELECT COUNT(*) FROM vehicles
                WHERE deleted_at IS NULL AND work_date = :d AND tipo = :t
            """),
            {"d": work_date_str, "t": tipo},
        ).scalar()
        return int(cnt or 0)
def read_secret(name, default=None):
    v = os.getenv(name)
    if v:
        return v
    try:
        return st.secrets[name]
    except Exception:
        return default

ADMIN_PASSWORD = read_secret("ADMIN_PASSWORD", "cambia_esto")
DATABASE_URL   = read_secret("DATABASE_URL", "sqlite:///vehiculos.db")

MAX_PER_DAY = 10  # límite total por día (suma comerciales + industriales)

# -------------------------
# Conexión BD (SQLAlchemy)
# -------------------------

def get_engine() -> Engine:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return engine

engine = get_engine()

# -------------------------
# Inicialización de tablas + migraciones simples
# -------------------------

DDL_VEHICLES_PG = text(
    """
    CREATE TABLE IF NOT EXISTS vehicles (
        id SERIAL PRIMARY KEY,
        modelo TEXT NOT NULL,
        bastidor TEXT NOT NULL,
        color TEXT NOT NULL,
        comercial TEXT NOT NULL,
        hora_prevista TEXT NOT NULL,
        matricula TEXT,
        comentarios TEXT,
        work_date TEXT,
        tipo TEXT DEFAULT 'Comercial',
        created_at TEXT NOT NULL,
        created_by TEXT NOT NULL,
        deleted_at TEXT,
        deleted_by TEXT,
        delete_reason TEXT
    );
    """
)

DDL_ACCESS_PG = text(
    """
    CREATE TABLE IF NOT EXISTS access_log (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        accessed_at TEXT NOT NULL
    );
    """
)

DDL_VEHICLES_SQLITE = text(
    """
    CREATE TABLE IF NOT EXISTS vehicles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        modelo TEXT NOT NULL,
        bastidor TEXT NOT NULL,
        color TEXT NOT NULL,
        comercial TEXT NOT NULL,
        hora_prevista TEXT NOT NULL,
        matricula TEXT,
        comentarios TEXT,
        work_date TEXT,
        tipo TEXT DEFAULT 'Comercial',
        created_at TEXT NOT NULL,
        created_by TEXT NOT NULL,
        deleted_at TEXT,
        deleted_by TEXT,
        delete_reason TEXT
    );
    """
)

DDL_ACCESS_SQLITE = text(
    """
    CREATE TABLE IF NOT EXISTS access_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        accessed_at TEXT NOT NULL
    );
    """
)


def init_db():
    is_sqlite = DATABASE_URL.startswith("sqlite:")
    with engine.begin() as conn:
        if is_sqlite:
            conn.execute(DDL_VEHICLES_SQLITE)
            conn.execute(DDL_ACCESS_SQLITE)
        else:
            conn.execute(DDL_VEHICLES_PG)
            conn.execute(DDL_ACCESS_PG)
        # Migraciones: asegurar columnas añadidas en versiones nuevas
        # work_date ya intentábamos crear antes
        try:
            conn.execute(text("ALTER TABLE vehicles ADD COLUMN work_date TEXT"))
        except Exception:
            pass
        # tipo: Comercial / Industrial
        try:
            conn.execute(text("ALTER TABLE vehicles ADD COLUMN tipo TEXT DEFAULT 'Comercial'"))
        except Exception:
            pass

# -------------------------
# Utilidades
# -------------------------

WEEKDAYS_ES = [
    "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"
]

def es_weekday_name(d: date) -> str:
    return WEEKDAYS_ES[d.weekday()]


def is_weekday(d: date) -> bool:
    return d.weekday() < 5  # 0..4 => Lun..Vie


# -------------------------
# Operaciones de datos
# -------------------------

def log_access(username: str):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO access_log(username, accessed_at) VALUES(:u, :t)"),
            {"u": username, "t": datetime.utcnow().isoformat()},
        )


def insert_vehicle(data: dict, user: str, work_date_str: str, tipo: str):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO vehicles(
                    modelo, bastidor, color, comercial, hora_prevista, matricula, comentarios, work_date, tipo, created_at, created_by
                ) VALUES (:modelo, :bastidor, :color, :comercial, :hora_prevista, :matricula, :comentarios, :work_date, :tipo, :created_at, :created_by)
                """
            ),
            {
                "modelo": data["modelo"].strip(),
                "bastidor": data["bastidor"].strip(),
                "color": data["color"].strip(),
                "comercial": data["comercial"].strip(),
                "hora_prevista": (data["hora_prevista"].strip() if data.get("hora_prevista") else None),
                "matricula": (data["matricula"] or "").strip(),
                "comentarios": (data["comentarios"] or "").strip(),
                "work_date": work_date_str,
                "tipo": (tipo or "Comercial"),
                "created_at": datetime.utcnow().isoformat(),
                "created_by": user,
            },
        )


def get_active_count(work_date_str: str) -> int:
    with engine.begin() as conn:
        cnt = conn.execute(
            text("SELECT COUNT(*) FROM vehicles WHERE deleted_at IS NULL AND work_date = :d"),
            {"d": work_date_str},
        ).scalar()
        return int(cnt or 0)


def get_active_df(work_date_str: str, tipo: str | None = None) -> pd.DataFrame:
    base = (
        """
        SELECT id as ID, modelo as Modelo, bastidor as Bastidor, color as Color,
               comercial as Comercial, hora_prevista as "Hora prevista",
               matricula as Matrícula, comentarios as Comentarios,
               tipo as Tipo, work_date as Fecha,
               created_at as "Creado en (UTC)", created_by as "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL AND work_date = %s
        """
    )
    if tipo:
        query = base + " AND tipo = %s ORDER BY id DESC"
        params = (work_date_str, tipo)
    else:
        query = base + " ORDER BY id DESC"
        params = (work_date_str,)
    return pd.read_sql(query, engine, params=params)


def get_all_df() -> pd.DataFrame:
    query = (
        """
        SELECT id as ID, modelo as Modelo, bastidor as Bastidor, color as Color,
               comercial as Comercial, hora_prevista as "Hora prevista",
               matricula as Matrícula, comentarios as Comentarios,
               tipo as Tipo, work_date as Fecha,
               created_at as "Creado en (UTC)", created_by as "Creado por",
               deleted_at as "Borrado en (UTC)", deleted_by as "Borrado por",
               delete_reason as "Motivo borrado"
        FROM vehicles
        ORDER BY id DESC
        """
    )
    return pd.read_sql(query, engine)


def soft_delete_vehicle(vehicle_id: int, admin_user: str, reason: str):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE vehicles
                SET deleted_at = :d, deleted_by = :u, delete_reason = :r
                WHERE id = :id AND deleted_at IS NULL
                """
            ),
            {"d": datetime.utcnow().isoformat(), "u": admin_user, "r": reason.strip(), "id": vehicle_id},
        )


# -------------------------
# UI (Streamlit)
# -------------------------

st.set_page_config(page_title="Control de vehículos", page_icon="🚚", layout="wide")

init_db()

def selector_fecha_sidebar():
    st.sidebar.divider()
    d_sel = st.sidebar.date_input("Fecha de trabajo (L-V)", value=date.today())
    weekday_name = es_weekday_name(d_sel)
    st.sidebar.write(f"**{weekday_name}**, {d_sel.strftime('%d-%m-%Y')}")
    if not is_weekday(d_sel):
        st.sidebar.error("Solo se permiten fechas de lunes a viernes.")
    return d_sel

st.title(f"🚚 Control de vehículos por día (máx. {MAX_PER_DAY} activos/día)")

# ---------------------------------
# PANTALLA 1 (PÚBLICA): listado por día + login sencillo
# ---------------------------------
if "user" not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    # Sidebar: selector de fecha + export público
    d_sel = selector_fecha_sidebar()
    work_date_str = d_sel.isoformat()

    # Listado público del día seleccionado (sin login)
    st.markdown("### Vehículos activos del día seleccionado (público)")
    df_public = get_active_df(work_date_str)
    if df_public.empty:
        st.info("No hay vehículos activos para esta fecha.")
    else:
        st.dataframe(df_public, use_container_width=True, hide_index=True)

    st.divider()
    username = st.text_input("Tu nombre (se registrará en el acceso y en altas)")
    if st.button("Entrar", type="primary"):
        if username.strip():
            st.session_state.user = username.strip()
            log_access(st.session_state.user)
            st.success(f"Bienvenido, {st.session_state.user}! Acceso registrado.")
            st.rerun()
        else:
            st.warning("Escribe un nombre válido.")
    st.stop()

# ---------------------------------
# PANTALLA 2 (TRAS LOGIN): pestañas Comerciales / Industriales
# ---------------------------------
with st.sidebar:
    st.caption(f"Conectado como: **{st.session_state.user}**")
    if st.button("Cambiar de usuario"):
        st.session_state.user = None
        st.rerun()

# Selector de fecha en sidebar (también en pantalla 2)
d_sel = selector_fecha_sidebar()
work_date_str = d_sel.isoformat()
weekday_name = es_weekday_name(d_sel)

col_info = st.container()
with col_info:
    count_c = get_count_by_type(work_date_str, "Comercial")
    count_i = get_count_by_type(work_date_str, "Industrial")
    total = count_c + count_i
    st.write(
        f"Registros {weekday_name} {d_sel.strftime('%d-%m-%Y')}: "
        f"**Comerciales {count_c}/{MAX_PER_TYPE['Comercial']}** · "
        f"**Industriales {count_i}/{MAX_PER_TYPE['Industrial']}** · "
        f"Total {total}"
    )

# Pestañas para tipos
pest_comercial, pest_industrial = st.tabs(["🚗 Comerciales", "🚛 Industriales"])

# --- Apartado: Comerciales ---
with pest_comercial:
    count_c = get_count_by_type(work_date_str, "Comercial")
    disabled = (count_c >= MAX_PER_TYPE["Comercial"]) or (not is_weekday(d_sel))
    st.caption(f"Comerciales activos hoy: {count_c}/{MAX_PER_TYPE['Comercial']}")
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add_comercial", clear_on_submit=True):
        st.subheader("Añadir vehículo comercial")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_c")
        bastidor = st.text_input("Bastidor", disabled=disabled, key="bas_c")
        color = st.text_input("Color", disabled=disabled, key="col_c")
        comercial_name = st.text_input("Comercial", disabled=disabled, key="com_c")
        hora_prevista = st.text_input("Hora prevista", placeholder="Ej: 10:30 o 2025-11-02 10:30", disabled=disabled, key="hor_c")
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled, key="mat_c")
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled, key="coments_c")
        submitted = st.form_submit_button("Guardar (Comercial)", disabled=disabled)
        if submitted:
            campos = {
                "modelo": modelo or "",
                "bastidor": bastidor or "",
                "color": color or "",
                "comercial": comercial_name or "",
                "hora_prevista": hora_prevista or "",
                "matricula": matricula or "",
                "comentarios": comentarios or "",
            }
            if not all(campos[k].strip() for k in ["modelo", "bastidor", "color", "comercial", ]):
                st.warning("Completa los campos obligatorios.")
            elif get_count_by_type(work_date_str, "Comercial") >= MAX_PER_TYPE["Comercial"]:
                st.error("Límite alcanzado para esta fecha. No se guardó.")
            else:
                insert_vehicle(campos, st.session_state.user, work_date_str, "Comercial")
                st.success("Vehículo comercial guardado.")
                st.rerun()

    st.markdown("### Comerciales activos en la fecha")
    df_c = get_active_df(work_date_str, "Comercial")
    st.dataframe(df_c, use_container_width=True, hide_index=True)

# --- Apartado: Industriales ---
with pest_industrial:
    count_i = get_count_by_type(work_date_str, "Industrial")
    disabled = (count_i >= MAX_PER_TYPE["Industrial"]) or (not is_weekday(d_sel))
    st.caption(f"Industriales activos hoy: {count_i}/{MAX_PER_TYPE['Industrial']}")

    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add_industrial", clear_on_submit=True):
        st.subheader("Añadir vehículo industrial")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_i")
        bastidor = st.text_input("Bastidor", disabled=disabled, key="bas_i")
        color = st.text_input("Color", disabled=disabled, key="col_i")
        comercial_name = st.text_input("Comercial", disabled=disabled, key="com_i")
        hora_prevista = st.text_input("Hora prevista", placeholder="Ej: 10:30 o 2025-11-02 10:30", disabled=disabled, key="hor_i")
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled, key="mat_i")
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled, key="coments_i")
        submitted = st.form_submit_button("Guardar (Industrial)", disabled=disabled)
        if submitted:
            campos = {
                "modelo": modelo or "",
                "bastidor": bastidor or "",
                "color": color or "",
                "comercial": comercial_name or "",
                "hora_prevista": hora_prevista or "",
                "matricula": matricula or "",
                "comentarios": comentarios or "",
            }
            if not all(campos[k].strip() for k in ["modelo", "bastidor", "color", "comercial", ]):
                st.warning("Completa los campos obligatorios.")
            elif get_count_by_type(work_date_str, "Industrial") >= MAX_PER_TYPE["Industrial"]:
                st.error("Límite alcanzado para esta fecha. No se guardó.")
            else:
                insert_vehicle(campos, st.session_state.user, work_date_str, "Industrial")
                st.success("Vehículo industrial guardado.")
                st.rerun()

    st.markdown("### Industriales activos en la fecha")
    df_i = get_active_df(work_date_str, "Industrial")
    st.dataframe(df_i, use_container_width=True, hide_index=True)

# --- Borrado con password ---
st.divider()
st.markdown("### Borrado con contraseña")
with st.expander("Borrar un vehículo (borrado lógico, requiere contraseña)"):
    vehicle_id = st.number_input("ID a borrar", min_value=1, step=1)
    reason = st.text_input("Motivo del borrado")
    admin_pass = st.text_input("Contraseña de administrador", type="password")
    if st.button("Borrar (lógico)"):
        if not reason.strip():
            st.warning("Debes indicar un motivo.")
        elif admin_pass != ADMIN_PASSWORD:
            st.error("Contraseña incorrecta.")
        else:
            soft_delete_vehicle(int(vehicle_id), st.session_state.user, reason)
            st.success("Vehículo marcado como borrado. (No se elimina físicamente)")
            st.rerun()

# --- Exportación e historial de accesos ---
st.sidebar.divider()
st.sidebar.markdown("**Exportar**")
all_df = get_all_df()
st.sidebar.download_button(
    label="Descargar CSV (todo el histórico)",
    data=all_df.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"vehiculos_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)

st.divider()
with st.expander("📜 Ver registro de accesos (requiere contraseña)"):
    admin_pass_log = st.text_input("Contraseña de administrador", type="password", key="access_log_pwd")
    if st.button("Ver registro", key="btn_access_log"):
        if admin_pass_log != ADMIN_PASSWORD:
            st.error("Contraseña incorrecta.")
        else:
            access_df = pd.read_sql(
                'SELECT id as ID, username as Usuario, accessed_at as "Accedido en (UTC)" FROM access_log ORDER BY id DESC',
                engine,
            )
            st.dataframe(access_df, use_container_width=True, hide_index=True)

st.caption("Hecho con ❤️ en Streamlit + SQLAlchemy. Listado público por fecha, altas por pestañas Comerciales/Industriales, límite por día y auditoría.")

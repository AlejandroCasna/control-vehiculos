

import os
from datetime import datetime, date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------
# Configuración/Secrets
# -------------------------

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
        work_date TEXT,               -- fecha (YYYY-MM-DD)
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
        # Migración mínima: asegurar columna work_date existe
        try:
            conn.execute(text("ALTER TABLE vehicles ADD COLUMN work_date TEXT"))
        except Exception:
            pass  # ya existe


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


def insert_vehicle(data: dict, user: str, work_date_str: str):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO vehicles(
                    modelo, bastidor, color, comercial, hora_prevista, matricula, comentarios, work_date, created_at, created_by
                ) VALUES (:modelo, :bastidor, :color, :comercial, :hora_prevista, :matricula, :comentarios, :work_date, :created_at, :created_by)
                """
            ),
            {
                "modelo": data["modelo"].strip(),
                "bastidor": data["bastidor"].strip(),
                "color": data["color"].strip(),
                "comercial": data["comercial"].strip(),
                "hora_prevista": data["hora_prevista"].strip(),
                "matricula": (data["matricula"] or "").strip(),
                "comentarios": (data["comentarios"] or "").strip(),
                "work_date": work_date_str,
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


def get_active_df(work_date_str: str) -> pd.DataFrame:
    query = """
        SELECT id as ID, modelo as Modelo, bastidor as Bastidor, color as Color,
               comercial as Comercial, hora_prevista as "Hora prevista",
               matricula as Matrícula, comentarios as Comentarios,
               work_date as Fecha,
               created_at as "Creado en (UTC)", created_by as "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL AND work_date = %s
        ORDER BY id DESC
    """
    return pd.read_sql(query, engine, params=(work_date_str,))



def get_all_df() -> pd.DataFrame:
    query = (
        """
        SELECT id as ID, modelo as Modelo, bastidor as Bastidor, color as Color,
               comercial as Comercial, hora_prevista as "Hora prevista",
               matricula as Matrícula, comentarios as Comentarios,
               work_date as Fecha,
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

st.set_page_config(page_title="Control de vehículos", page_icon="🚙", layout="wide")

init_db()

st.title("🚙 Control de vehículos por día (máx. 15 activos/día)")

st.info(
    "ℹ️ **Producción recomendada**: Postgres gestionado (Neon) con `DATABASE_URL` en Secrets."
    "Esta app guarda cada registro asociado a una **fecha laboral (L-V)**."
)

# --- Identificación (no requiere contraseña para ver el registro de accesos) ---
if "user" not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.subheader("Identifícate")
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

# --- Selección de fecha laboral (L-V) ---
with st.sidebar:
    st.caption(f"Conectado como: **{st.session_state.user}**")
    if st.button("Cambiar de usuario"):
        st.session_state.user = None
        st.rerun()

    st.divider()
    d_sel = st.date_input("Fecha de trabajo (L-V)", value=date.today())
    weekday_name = es_weekday_name(d_sel)
    st.write(f"**{weekday_name}**, {d_sel.strftime('%d-%m-%Y')}")

    only_weekdays = is_weekday(d_sel)
    if not only_weekdays:
        st.error("Solo se permiten fechas de lunes a viernes.")

    st.divider()
    st.markdown("**Exportar**")
    all_df = get_all_df()
    st.download_button(
        label="Descargar CSV (todo el histórico)",
        data=all_df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"vehiculos_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )

# --- Columnas principales ---
col_form, col_tabla = st.columns([1, 2], gap="large")

work_date_str = d_sel.isoformat()

with col_form:
    st.subheader("Añadir vehículo")

    activos = get_active_count(work_date_str)
    st.write(f"Registros activos en {weekday_name} {d_sel.strftime('%d-%m-%Y')}: **{activos}/15**")

    disabled = (activos >= 15) or (not only_weekdays)
    if activos >= 15:
        st.error("Has alcanzado el máximo de 15 vehículos activos para esta fecha.")
    if not only_weekdays:
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add", clear_on_submit=True):
        modelo = st.text_input("Modelo", disabled=disabled)
        bastidor = st.text_input("Bastidor", disabled=disabled)
        color = st.text_input("Color", disabled=disabled)
        comercial = st.text_input("Comercial", disabled=disabled)
        hora_prevista = st.text_input("Hora prevista", placeholder="Ej: 10:30 o 2025-11-02 10:30", disabled=disabled)
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled)
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled)

        submitted = st.form_submit_button("Guardar", disabled=disabled)
        if submitted:
            campos = {
                "modelo": modelo or "",
                "bastidor": bastidor or "",
                "color": color or "",
                "comercial": comercial or "",
                "hora_prevista": hora_prevista or "",
                "matricula": matricula or "",
                "comentarios": comentarios or "",
            }
            if not all(campos[k].strip() for k in ["modelo", "bastidor", "color", "comercial", "hora_prevista"]):
                st.warning("Completa los campos obligatorios.")
            else:
                if get_active_count(work_date_str) >= 15:
                    st.error("Límite alcanzado para esta fecha. No se guardó.")
                elif not only_weekdays:
                    st.error("Fecha no laboral. No se guardó.")
                else:
                    insert_vehicle(campos, st.session_state.user, work_date_str)
                    st.success("Vehículo guardado.")
                    st.rerun()

with col_tabla:
    st.subheader("Vehículos activos en la fecha seleccionada")
    df = get_active_df(work_date_str)
    if df.empty:
        st.info("No hay vehículos activos para esta fecha.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

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

st.caption("Hecho con ❤️ en Streamlit + PostGres por Alejandro. Historiza accesos y acciones de borrado. Registros por fecha laboral (L-V).")

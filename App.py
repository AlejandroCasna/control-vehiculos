



import os
from datetime import datetime, date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine



def get_active_df(work_date_str: str, tipo: str | None = None) -> pd.DataFrame:
    base = (
        """
        SELECT id as ID, modelo as Modelo, bastidor as Bastidor, color as Color,
               comercial as Comercial, hora_prevista as "Hora prevista",
               matricula as Matrícula, comentarios as Comentarios,
               placa as Placa, kit as Kit, alfombrillas as Alfombrillas,
               done as Hecho,
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

def style_done(df: pd.DataFrame):
    if df.empty or "Hecho" not in df.columns:
        return df
    def _row_style(row):
        return ['background-color: #e8ffe8'] * len(row) if bool(row.get("Hecho", False)) else [''] * len(row)
    return df.style.apply(_row_style, axis=1)
# -------------------------
# Configuración/Secrets
# ------------------------- 
MAX_PER_TYPE = {"Turismo": 15, "Industrial": 15}

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
        # 1) Crear tablas (según motor)
        if is_sqlite:
            conn.execute(DDL_VEHICLES_SQLITE)
            conn.execute(DDL_ACCESS_SQLITE)
        else:
            conn.execute(DDL_VEHICLES_PG)
            conn.execute(DDL_ACCESS_PG)

        # 2) Migraciones idempotentes (no fallan si ya existen)

        # 2.1 work_date
        try:
            conn.execute(text("ALTER TABLE vehicles ADD COLUMN work_date TEXT"))
        except Exception:
            pass

        # 2.2 tipo -> asegurar columna y dejar 'Turismo' como default/valor
        try:
            conn.execute(text("ALTER TABLE vehicles ADD COLUMN tipo TEXT"))
        except Exception:
            pass
        # Normaliza valores existentes
        try:
            conn.execute(text("UPDATE vehicles SET tipo = 'Turismo' WHERE tipo IS NULL OR tipo = 'Comercial'"))
        except Exception:
            pass
        # Default sólo en Postgres (SQLite no soporta ALTER COLUMN SET DEFAULT igual)
        if not is_sqlite:
            try:
                conn.execute(text("ALTER TABLE vehicles ALTER COLUMN tipo SET DEFAULT 'Turismo'"))
            except Exception:
                pass

        # 2.3 hora_prevista nullable (sólo Postgres)
        if not is_sqlite:
            try:
                conn.execute(text("ALTER TABLE vehicles ALTER COLUMN hora_prevista DROP NOT NULL"))
            except Exception:
                pass
        # En SQLite ya la definimos como TEXT (sin NOT NULL) en el DDL.

        # 2.4 columnas nuevas de check y “hecho”
        for coldef in [
            "placa BOOLEAN DEFAULT FALSE",
            "kit BOOLEAN DEFAULT FALSE",
            "alfombrillas BOOLEAN DEFAULT FALSE",
            "done BOOLEAN DEFAULT FALSE",
            "done_at TEXT",
            "done_by TEXT",
        ]:
            try:
                conn.execute(text(f"ALTER TABLE vehicles ADD COLUMN {coldef}"))
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
            text("""
                    INSERT INTO vehicles(
                    modelo, bastidor, color, comercial, hora_prevista, matricula, comentarios,
                    work_date, tipo, placa, kit, alfombrillas, done, created_at, created_by
                    ) VALUES (
                    :modelo, :bastidor, :color, :comercial, :hora_prevista, :matricula, :comentarios,
                    :work_date, :tipo, :placa, :kit, :alfombrillas, FALSE, :created_at, :created_by
                    )
                    """),
                    {
                    "modelo": data["modelo"].strip(),
                    "bastidor": data["bastidor"].strip(),
                    "color": data["color"].strip(),
                    "comercial": data["comercial"].strip(),
                    "hora_prevista": (data["hora_prevista"].strip() if data.get("hora_prevista") else None),
                    "matricula": (data["matricula"] or "").strip(),
                    "comentarios": (data["comentarios"] or "").strip(),
                    "work_date": work_date_str,
                    "tipo": tipo or "Turismo",
                    "placa": bool(data.get("placa")),
                    "kit": bool(data.get("kit")),
                    "alfombrillas": bool(data.get("alfombrillas")),
                    "created_at": datetime.utcnow().isoformat(),
                    "created_by": user,
                    }
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
               placa as Placa, kit as Kit, alfombrillas as Alfombrillas,
               done as Hecho,
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
    st.subheader("Identifícate")
    # Sidebar: selector de fecha + export público
    d_sel = selector_fecha_sidebar()
    work_date_str = d_sel.isoformat()

    # Listado público del día seleccionado (sin login)
    st.markdown("### Vehículos activos del día seleccionado (público)")
    df_public = get_active_df(work_date_str)  # 👈 ahora sí dentro del bloque
    if df_public.empty:
        st.info("No hay vehículos activos para esta fecha.")
    else:
        st.caption("Las filas en verde están marcadas como 'Hecho'.")
        st.dataframe(style_done(df_public), use_container_width=True, hide_index=True)

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
    count_t = get_count_by_type(work_date_str, "Turismo")
    count_i = get_count_by_type(work_date_str, "Industrial")
    total = count_t + count_i
    st.write(
        f"Registros {weekday_name} {d_sel.strftime('%d-%m-%Y')}: "
        f"**Turismo {count_t}/{MAX_PER_TYPE['Turismo']}** · "
        f"**Industriales {count_i}/{MAX_PER_TYPE['Industrial']}** · "
        f"Total {total}"
    )

# Pestañas para tipos
pest_turismo, pest_industrial = st.tabs(["🚘 Turismo", "🚛 Industriales"])


# --- Apartado: Comerciales ---
with pest_turismo:
    count_t = get_count_by_type(work_date_str, "Turismo")
    disabled = (count_t >= MAX_PER_TYPE["Turismo"]) or (not is_weekday(d_sel))
    st.caption(f"Turismo activos hoy: {count_t}/{MAX_PER_TYPE['Turismo']}")
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add_turismo", clear_on_submit=True):
        st.subheader("Añadir vehículo (Turismo)")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_t")
        bastidor = st.text_input("Bastidor", disabled=disabled, key="bas_t")
        color = st.text_input("Color", disabled=disabled, key="col_t")
        comercial_name = st.text_input("Comercial", disabled=disabled, key="com_t")
        hora_prevista = st.text_input("Hora prevista", placeholder="Ej: 10:30 o 2025-11-02 10:30", disabled=disabled, key="hor_t")
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled, key="mat_t")
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled, key="coments_t")

        # ✅ los checkboxes deben ir ANTES del submit (para que se guarden)
        placa = st.checkbox("Placa", value=False, disabled=disabled, key="placa_t")
        kit = st.checkbox("Kit", value=False, disabled=disabled, key="kit_t")
        alfombrillas = st.checkbox("Alfombrillas", value=False, disabled=disabled, key="alf_t")

        submitted = st.form_submit_button("Guardar (Turismo)", disabled=disabled)
        if submitted:
            campos = {
                "modelo": modelo or "",
                "bastidor": bastidor or "",
                "color": color or "",
                "comercial": comercial_name or "",
                "hora_prevista": hora_prevista or "",
                "matricula": matricula or "",
                "comentarios": comentarios or "",
                "placa": placa,
                "kit": kit,
                "alfombrillas": alfombrillas,
            }
            if not all(campos[k].strip() for k in ["modelo", "bastidor", "color", "comercial"]):
                st.warning("Completa los campos obligatorios.")
            elif get_count_by_type(work_date_str, "Turismo") >= MAX_PER_TYPE["Turismo"]:
                st.error("Límite alcanzado para esta fecha. No se guardó.")
            else:
                insert_vehicle(campos, st.session_state.user, work_date_str, "Turismo")
                st.success("Vehículo (Turismo) guardado.")
                st.rerun()

    st.markdown("### Turismo activos en la fecha")
    df_t = get_active_df(work_date_str, "Turismo")
if df_t.empty:
    st.info("No hay turismo para esta fecha.")
else:
    st.caption("Verde = 'Hecho'")
    st.dataframe(style_done(df_t), use_container_width=True, hide_index=True)


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

        placa_i = st.checkbox("Placa", value=False, disabled=disabled, key="placa_i")
        kit_i = st.checkbox("Kit", value=False, disabled=disabled, key="kit_i")
        alfombrillas_i = st.checkbox("Alfombrillas", value=False, disabled=disabled, key="alf_i")

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
                "placa": placa_i,
                "kit": kit_i,
                "alfombrillas": alfombrillas_i,
            }
            if not all(campos[k].strip() for k in ["modelo", "bastidor", "color", "comercial"]):
                st.warning("Completa los campos obligatorios.")
            elif get_count_by_type(work_date_str, "Industrial") >= MAX_PER_TYPE["Industrial"]:
                st.error("Límite alcanzado para esta fecha. No se guardó.")
            else:
                insert_vehicle(campos, st.session_state.user, work_date_str, "Industrial")
                st.success("Vehículo industrial guardado.")
                st.rerun()

    st.markdown("### Industriales activos en la fecha")
df_i = get_active_df(work_date_str, "Industrial")



if df_i.empty:
    st.info("No hay industriales para esta fecha.")
else:
    st.caption("Verde = 'Hecho'")
    st.dataframe(style_done(df_i), use_container_width=True, hide_index=True)


# --- Borrado con password ---
st.divider()
st.markdown("### Borrado con contraseña")
with st.expander("✅ Marcar como hechos (requiere contraseña)"):
    admin_name = st.text_input("Tu nombre (se guardará como 'hecho por')", key="mark_name")
    admin_pass = st.text_input("Contraseña", type="password", key="mark_pwd")

    if admin_pass == ADMIN_PASSWORD:
        df_edit = get_active_df(work_date_str).copy()
        if df_edit.empty:
            st.info("No hay vehículos para esta fecha.")
        else:
            # 🔒 Defensa: crear columnas faltantes si no existen
            if "Hecho" not in df_edit.columns:
                df_edit["Hecho"] = False
            if "Hora prevista" not in df_edit.columns:
                df_edit["Hora prevista"] = ""
        
            cols_needed = ["ID", "Modelo", "Bastidor", "Hora prevista", "Hecho"]
            cols_present = [c for c in cols_needed if c in df_edit.columns]
            if len(cols_present) < len(cols_needed):
                st.warning(f"Faltan columnas para editar: {set(cols_needed) - set(cols_present)}. Se usarán las disponibles.")
            editable = df_edit[cols_present].copy()
        
            edited = st.data_editor(
                editable,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "Hecho": st.column_config.CheckboxColumn("Hecho", help="Marcar como completado"),
                } if "Hecho" in editable.columns else {},
                key="editor_hechos",
            )

            if st.button("Guardar cambios de 'Hecho'"):
                original = editable.set_index("ID")
                changed_ids = []
                for _, row in edited.iterrows():
                    vid = int(row["ID"])
                    new_done = bool(row["Hecho"])
                    old_done = bool(original.loc[vid, "Hecho"])
                    if new_done != old_done:
                        changed_ids.append((vid, new_done))

                if changed_ids:
                    who = (admin_name or "admin").strip()
                    when = datetime.utcnow().isoformat()
                    with engine.begin() as conn:
                        for vid, new_done in changed_ids:
                            conn.execute(
                                text("""
                                    UPDATE vehicles
                                    SET done = :d,
                                        done_at = CASE WHEN :d THEN :dt ELSE done_at END,
                                        done_by = CASE WHEN :d THEN :by ELSE done_by END
                                    WHERE id = :id
                                """),
                                {"d": new_done, "dt": when, "by": who, "id": vid},
                            )
                    st.success(f"Actualizados {len(changed_ids)} registro(s).")
                    st.rerun()
    else:
        if admin_pass:
            st.error("Contraseña incorrecta.")


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
with st.expander("Marcar como hechos (requiere contraseña)"):
    admin_name = st.text_input("Tu nombre (se guardará como 'hecho por')", key="mark_name")
    admin_pass = st.text_input("Contraseña", type="password", key="mark_pwd")
    if admin_pass == ADMIN_PASSWORD:
        # dataframe editable con la columna Hecho
        df_edit = get_active_df(work_date_str).copy()
        if not df_edit.empty:
            # Usaremos solo columnas necesarias
            editable = df_edit[["ID", "Modelo", "Bastidor", "Hora prevista", "Hecho"]].copy()
            edited = st.data_editor(
                editable,
                use_container_width=True,
                num_rows="fixed",
                key="editor_hechos",
            )
            if st.button("Guardar cambios de 'Hecho'"):
                # Compara edited vs original
                original = editable.set_index("ID")
                changed_ids = []
                for _, row in edited.iterrows():
                    vid = int(row["ID"])
                    new_done = bool(row["Hecho"])
                    old_done = bool(original.loc[vid, "Hecho"])
                    if new_done != old_done:
                        changed_ids.append((vid, new_done))
                if changed_ids:
                    who = (admin_name or "admin").strip()
                    when = datetime.utcnow().isoformat()
                    with engine.begin() as conn:
                        for vid, new_done in changed_ids:
                            conn.execute(
                                text("""
                                    UPDATE vehicles
                                    SET done = :d, 
                                        done_at = CASE WHEN :d THEN :dt ELSE done_at END,
                                        done_by = CASE WHEN :d THEN :by ELSE done_by END
                                    WHERE id = :id
                                """),
                                {"d": new_done, "dt": when, "by": who, "id": vid},
                            )
                    st.success(f"Actualizados {len(changed_ids)} registro(s).")
                    st.rerun()
        else:
            st.info("No hay vehículos para esta fecha.")
    else:
        if admin_pass:
            st.error("Contraseña incorrecta.")
st.caption("Hecho con ❤️ en Streamlit + SQLAlchemy. Listado público por fecha, altas por pestañas Comerciales/Industriales, límite por día y auditoría.")

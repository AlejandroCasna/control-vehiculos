# App.py
import os
from datetime import datetime, date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =========================
# Configuración / Secrets
# =========================
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

MAX_PER_DAY = 15  # límite TOTAL por día (Turismo + Industrial)

# =========================
# Conexión BD
# =========================
def get_engine() -> Engine:
    return create_engine(DATABASE_URL, pool_pre_ping=True)

engine = get_engine()

# =========================
# DDL
# =========================
DDL_VEHICLES_PG = text("""
CREATE TABLE IF NOT EXISTS vehicles (
    id SERIAL PRIMARY KEY,
    modelo TEXT NOT NULL,
    bastidor TEXT NOT NULL,
    color TEXT NOT NULL,
    comercial TEXT NOT NULL,
    hora_prevista TEXT,
    matricula TEXT,
    comentarios TEXT,
    work_date TEXT,
    tipo TEXT DEFAULT 'Turismo',
    placa BOOLEAN DEFAULT FALSE,
    kit BOOLEAN DEFAULT FALSE,
    alfombrillas BOOLEAN DEFAULT FALSE,
    done BOOLEAN DEFAULT FALSE,
    done_at TEXT,
    done_by TEXT,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    deleted_at TEXT,
    deleted_by TEXT,
    delete_reason TEXT
);
""")

DDL_ACCESS_PG = text("""
CREATE TABLE IF NOT EXISTS access_log (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    accessed_at TEXT NOT NULL
);
""")

DDL_VEHICLES_SQLITE = text("""
CREATE TABLE IF NOT EXISTS vehicles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    modelo TEXT NOT NULL,
    bastidor TEXT NOT NULL,
    color TEXT NOT NULL,
    comercial TEXT NOT NULL,
    hora_prevista TEXT,
    matricula TEXT,
    comentarios TEXT,
    work_date TEXT,
    tipo TEXT DEFAULT 'Turismo',
    placa BOOLEAN DEFAULT FALSE,
    kit BOOLEAN DEFAULT FALSE,
    alfombrillas BOOLEAN DEFAULT FALSE,
    done BOOLEAN DEFAULT FALSE,
    done_at TEXT,
    done_by TEXT,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    deleted_at TEXT,
    deleted_by TEXT,
    delete_reason TEXT
);
""")

DDL_ACCESS_SQLITE = text("""
CREATE TABLE IF NOT EXISTS access_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    accessed_at TEXT NOT NULL
);
""")

def init_db():
    is_sqlite = DATABASE_URL.startswith("sqlite:")
    with engine.begin() as conn:
        if is_sqlite:
            conn.execute(DDL_VEHICLES_SQLITE)
            conn.execute(DDL_ACCESS_SQLITE)
        else:
            conn.execute(DDL_VEHICLES_PG)
            conn.execute(DDL_ACCESS_PG)

        # Migraciones idempotentes
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN work_date TEXT"))
        except Exception: pass
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN tipo TEXT"))
        except Exception: pass
        try: conn.execute(text("UPDATE vehicles SET tipo='Turismo' WHERE tipo IS NULL OR tipo='Comercial'"))
        except Exception: pass
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN placa BOOLEAN DEFAULT FALSE"))
        except Exception: pass
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN kit BOOLEAN DEFAULT FALSE"))
        except Exception: pass
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN alfombrillas BOOLEAN DEFAULT FALSE"))
        except Exception: pass
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN done BOOLEAN DEFAULT FALSE"))
        except Exception: pass
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN done_at TEXT"))
        except Exception: pass
        try: conn.execute(text("ALTER TABLE vehicles ADD COLUMN done_by TEXT"))
        except Exception: pass

# =========================
# Utilidades
# =========================
WEEKDAYS_ES = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
def es_weekday_name(d: date) -> str: return WEEKDAYS_ES[d.weekday()]
def is_weekday(d: date) -> bool: return d.weekday() < 5

def style_done(df: pd.DataFrame):
    # Intento de pintar filas si tu versión de Streamlit acepta Styler
    if df.empty: return df
    colmap = {c.lower(): c for c in df.columns}
    if "hecho" not in colmap: return df
    col_hecho = colmap["hecho"]
    def _row_style(row):
        try: done = bool(row[col_hecho])
        except Exception: done = False
        return ['background-color: #e8ffe8' if done else '' for _ in row]
    return df.style.apply(_row_style, axis=1)

# =========================
# Operaciones de datos
# =========================
def log_access(username: str):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO access_log(username, accessed_at) VALUES(:u,:t)"),
                     {"u": username, "t": datetime.utcnow().isoformat()})

def insert_vehicle(data: dict, user: str, work_date_str: str, tipo: str):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO vehicles(
                modelo,bastidor,color,comercial,hora_prevista,matricula,comentarios,
                work_date,tipo,placa,kit,alfombrillas,done,created_at,created_by
            ) VALUES (
                :modelo,:bastidor,:color,:comercial,:hora_prevista,:matricula,:comentarios,
                :work_date,:tipo,:placa,:kit,:alfombrillas,FALSE,:created_at,:created_by
            )
        """), {
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
        })

def get_active_count(work_date_str: str) -> int:
    with engine.begin() as conn:
        cnt = conn.execute(text("""
            SELECT COUNT(*) FROM vehicles
            WHERE deleted_at IS NULL AND work_date = :d
        """), {"d": work_date_str}).scalar()
        return int(cnt or 0)

def get_active_df(work_date_str: str, tipo: str | None = None) -> pd.DataFrame:
    base = """
        SELECT id as ID, modelo as Modelo, bastidor as Bastidor, color as Color,
               comercial as Comercial, hora_prevista as "Hora prevista",
               matricula as Matrícula, comentarios as Comentarios,
               placa as Placa, kit as Kit, alfombrillas as Alfombrillas,
               done as Hecho, tipo as Tipo, work_date as Fecha,
               created_at as "Creado en (UTC)", created_by as "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL AND work_date = %s
    """
    if tipo:
        query = base + " AND tipo = %s ORDER BY id DESC"
        params = (work_date_str, tipo)
    else:
        query = base + " ORDER BY id DESC"
        params = (work_date_str,)
    df = pd.read_sql(query, engine, params=params)
    # Columna de estado visible siempre
    if "Hecho" in df.columns:
        df.insert(0, "Estado", df["Hecho"].map(lambda x: "Hecho ✅" if bool(x) else "Pendiente ⏳"))
    return df

def get_active_all_df(date_from: str | None = None,
                      date_to: str | None = None,
                      tipo: str | None = None) -> pd.DataFrame:
    base = """
        SELECT id as ID, modelo as Modelo, bastidor as Bastidor, color as Color,
               comercial as Comercial, hora_prevista as "Hora prevista",
               matricula as Matrícula, comentarios as Comentarios,
               placa as Placa, kit as Kit, alfombrillas as Alfombrillas,
               done as Hecho, tipo as Tipo, work_date as Fecha,
               created_at as "Creado en (UTC)", created_by as "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL
    """
    conds, params = [], []
    if date_from: conds.append("AND work_date >= %s"); params.append(date_from)
    if date_to:   conds.append("AND work_date <= %s"); params.append(date_to)
    if tipo in ("Turismo", "Industrial"): conds.append("AND tipo = %s"); params.append(tipo)
    query = base + " " + " ".join(conds) + " ORDER BY work_date DESC, id DESC"
    df = pd.read_sql(query, engine, params=tuple(params))
    if "Hecho" in df.columns:
        df.insert(0, "Estado", df["Hecho"].map(lambda x: "Hecho ✅" if bool(x) else "Pendiente ⏳"))
    return df

def get_all_df() -> pd.DataFrame:
    query = """
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
    return pd.read_sql(query, engine)

def soft_delete_vehicle(vehicle_id: int, admin_user: str, reason: str):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE vehicles
            SET deleted_at=:d, deleted_by=:u, delete_reason=:r
            WHERE id=:id AND deleted_at IS NULL
        """), {"d": datetime.utcnow().isoformat(), "u": admin_user,
               "r": reason.strip(), "id": vehicle_id})

# =========================
# UI
def normaliza_columnas(df: pd.DataFrame) -> pd.DataFrame:
    # Mapea por nombre en minúscula -> nombre canónico
    mapa = {
        "id": "ID",
        "modelo": "Modelo",
        "bastidor": "Bastidor",
        "color": "Color",
        "comercial": "Comercial",
        "hora prevista": "Hora prevista",
        "matricula": "Matrícula",
        "matrícula": "Matrícula",
        "comentarios": "Comentarios",
        "placa": "Placa",
        "kit": "Kit",
        "alfombrillas": "Alfombrillas",
        "hecho": "Hecho",
        "tipo": "Tipo",
        "fecha": "Fecha",
        "work_date": "Fecha",
        "creado en (utc)": "Creado en (UTC)",
        "creado por": "Creado por",
    }
    ren = {c: mapa.get(c.lower(), c) for c in df.columns}
    return df.rename(columns=ren)

# =========================
st.set_page_config(page_title="Control de vehículos", page_icon="🚚", layout="wide")
init_db()

def selector_fecha_sidebar():
    st.sidebar.divider()
    d_sel = st.sidebar.date_input("Fecha de trabajo (L-V)", value=date.today(), key="fecha_work")
    weekday_name = es_weekday_name(d_sel)
    st.sidebar.write(f"**{weekday_name}**, {d_sel.strftime('%d-%m-%Y')}")
    if not is_weekday(d_sel):
        st.sidebar.error("Solo se permiten fechas de lunes a viernes.")
    return d_sel

st.title("🚚 Control de vehículos por día (máx. 15 coches/día)")

# ---- PANTALLA 1 (pública)
if "user" not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.subheader("Identifícate")
    d_sel = selector_fecha_sidebar()
    work_date_str = d_sel.isoformat()

    st.markdown("### Vehículos activos del día seleccionado (público)")
    df_public = get_active_df(work_date_str)
    if df_public.empty:
        st.info("No hay vehículos activos para esta fecha.")
    else:
        st.caption("La columna **Estado** indica si está hecho. (Si tu versión lo permite, también se verá en verde).")
        # mostramos tanto con Styler (si aplica) como sin
        try:
            st.dataframe(style_done(df_public), use_container_width=True, hide_index=True)
        except Exception:
            st.dataframe(df_public, use_container_width=True, hide_index=True)

    st.divider()
    username = st.text_input("Tu nombre (se registrará en el acceso y en altas)", key="login_name")
    if st.button("Entrar", type="primary", key="login_btn"):
        if (username or "").strip():
            st.session_state.user = username.strip()
            log_access(st.session_state.user)
            st.success(f"Bienvenido, {st.session_state.user}! Acceso registrado.")
            st.rerun()
        else:
            st.warning("Escribe un nombre válido.")
    st.stop()

# ---- PANTALLA 2 (tras login)
with st.sidebar:
    st.caption(f"Conectado como: **{st.session_state.user}**")
    if st.button("Cambiar de usuario", key="swap_user"):
        st.session_state.user = None
        st.rerun()

d_sel = selector_fecha_sidebar()
work_date_str = d_sel.isoformat()
weekday_name = es_weekday_name(d_sel)

activos_total = get_active_count(work_date_str)
st.write(f"Registros activos en **{weekday_name} {d_sel.strftime('%d-%m-%Y')}**: **{activos_total}/{MAX_PER_DAY}**")
if activos_total >= MAX_PER_DAY:
    st.error("Has alcanzado el máximo de vehículos activos para esta fecha.")

pest_turismo, pest_industrial = st.tabs(["🚘 Turismo", "🚛 Industriales"])

# ---- TURISMO
with pest_turismo:
    disabled = (activos_total >= MAX_PER_DAY) or (not is_weekday(d_sel))
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add_turismo", clear_on_submit=True):
        st.subheader("Añadir vehículo (Turismo)")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_t")
        # Bastidor EXACTO 8 caracteres
        bastidor_raw = st.text_input("Bastidor (8 caracteres)", disabled=disabled, key="bas_t", max_chars=8)
        bastidor = (bastidor_raw or "").strip()
        color = st.text_input("Color", disabled=disabled, key="col_t")
        comercial_name = st.text_input("Comercial", disabled=disabled, key="com_t")
        hora_prevista = st.text_input("Hora prevista", placeholder="Ej: 10:30 o 2025-11-02 10:30", disabled=disabled, key="hor_t")
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled, key="mat_t")
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled, key="coments_t")
        placa = st.checkbox("Placa", value=False, disabled=disabled, key="placa_t")
        kit = st.checkbox("Kit", value=False, disabled=disabled, key="kit_t")
        alfombrillas = st.checkbox("Alfombrillas", value=False, disabled=disabled, key="alf_t")

        submitted = st.form_submit_button("Guardar (Turismo)", disabled=disabled)
        if submitted:
            if not all([(modelo or "").strip(), bastidor, (color or "").strip(), (comercial_name or "").strip()]):
                st.warning("Completa los campos obligatorios.")
            elif len(bastidor) != 8:
                st.error("El bastidor debe tener **exactamente 8 caracteres**.")
            elif get_active_count(work_date_str) >= MAX_PER_DAY:
                st.error("Límite global alcanzado para esta fecha. No se guardó.")
            else:
                campos = {
                    "modelo": modelo or "",
                    "bastidor": bastidor,
                    "color": color or "",
                    "comercial": comercial_name or "",
                    "hora_prevista": hora_prevista or "",
                    "matricula": matricula or "",
                    "comentarios": comentarios or "",
                    "placa": placa, "kit": kit, "alfombrillas": alfombrillas,
                }
                insert_vehicle(campos, st.session_state.user, work_date_str, "Turismo")
                st.success("Vehículo (Turismo) guardado.")
                st.rerun()

    st.markdown("### Turismo activos en la fecha")
    df_t = get_active_df(work_date_str, "Turismo")
    if df_t.empty:
        st.info("No hay Turismo para esta fecha.")
    else:
        try:
            st.dataframe(style_done(df_t), use_container_width=True, hide_index=True)
        except Exception:
            st.dataframe(df_t, use_container_width=True, hide_index=True)

# ---- INDUSTRIAL
with pest_industrial:
    disabled = (get_active_count(work_date_str) >= MAX_PER_DAY) or (not is_weekday(d_sel))
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add_industrial", clear_on_submit=True):
        st.subheader("Añadir vehículo (Industrial)")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_i")
        bastidor_raw = st.text_input("Bastidor (8 caracteres)", disabled=disabled, key="bas_i", max_chars=8)
        bastidor = (bastidor_raw or "").strip()
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
            if not all([(modelo or "").strip(), bastidor, (color or "").strip(), (comercial_name or "").strip()]):
                st.warning("Completa los campos obligatorios.")
            elif len(bastidor) != 8:
                st.error("El bastidor debe tener **exactamente 8 caracteres**.")
            elif get_active_count(work_date_str) >= MAX_PER_DAY:
                st.error("Límite global alcanzado para esta fecha. No se guardó.")
            else:
                campos = {
                    "modelo": modelo or "",
                    "bastidor": bastidor,
                    "color": color or "",
                    "comercial": comercial_name or "",
                    "hora_prevista": hora_prevista or "",
                    "matricula": matricula or "",
                    "comentarios": comentarios or "",
                    "placa": placa_i, "kit": kit_i, "alfombrillas": alfombrillas_i,
                }
                insert_vehicle(campos, st.session_state.user, work_date_str, "Industrial")
                st.success("Vehículo (Industrial) guardado.")
                st.rerun()

    st.markdown("### Industriales activos en la fecha")
    df_i = get_active_df(work_date_str, "Industrial")
    if df_i.empty:
        st.info("No hay Industriales para esta fecha.")
    else:
        try:
            st.dataframe(style_done(df_i), use_container_width=True, hide_index=True)
        except Exception:
            st.dataframe(df_i, use_container_width=True, hide_index=True)

st.divider()

# ---- Exportación + Accesos
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

st.divider()
with st.expander("🔐 Admin – marcar coches terminados (todos los días)"):
    admin_pass_admin = st.text_input("Contraseña de administrador", type="password", key="admin_panel_pwd")

    if admin_pass_admin != ADMIN_PASSWORD:
        if admin_pass_admin:
            st.error("Contraseña incorrecta.")
        st.stop()

    st.success("Acceso concedido.")

    # Filtros opcionales
    colf1, colf2, colf3 = st.columns([1,1,1])
    with colf1:
        usar_desde = st.checkbox("Filtrar desde", value=False, key="f_desde")
        df_desde = st.date_input("Desde", value=date.today(), key="desde") if usar_desde else None
    with colf2:
        usar_hasta = st.checkbox("Filtrar hasta", value=False, key="f_hasta")
        df_hasta = st.date_input("Hasta", value=date.today(), key="hasta") if usar_hasta else None
    with colf3:
        tipo_sel = st.selectbox("Tipo", ["Todos", "Turismo", "Industrial"], index=0, key="tipo_admin")

    date_from = df_desde.isoformat() if df_desde else None
    date_to   = df_hasta.isoformat() if df_hasta else None
    tipo_arg  = None if tipo_sel == "Todos" else tipo_sel

    df_admin = get_active_all_df(date_from=date_from, date_to=date_to, tipo=tipo_arg)

    if df_admin.empty:
        st.info("No hay coches activos con esos filtros.")
        st.stop()

    st.caption("La columna **Estado** muestra Hecho/Pendiente. (Si tu versión lo permite, filas hechas en verde).")
    try:
        st.dataframe(style_done(df_admin), use_container_width=True, hide_index=True)
    except Exception:
        st.dataframe(df_admin, use_container_width=True, hide_index=True)

    st.markdown("#### Editar estado 'Hecho'")

    # Preparamos solo las columnas necesarias
    cols_needed = ["ID", "Modelo", "Bastidor", "Hora prevista", "Hecho", "Tipo", "Fecha"]
    editable_cols = [c for c in cols_needed if c in df_admin.columns]
    df_editable = df_admin[editable_cols].copy()

    # Aseguramos existencia de Hecho (por si acaso)
    if "Hecho" not in df_editable.columns:
        df_editable["Hecho"] = False

    # ✅ Usamos ID como índice ANTES del editor para no depender de que siga en columns
    if "ID" in df_editable.columns:
        df_editable = df_editable.set_index("ID", drop=True)
    else:
        # Si por alguna razón no vino ID, no podemos editar con seguridad
        st.error("No se encontró la columna ID en los datos del administrador.")
        st.stop()

    # Guardamos original para diff (solo la columna Hecho)
    original_done = df_editable["Hecho"].astype(bool).copy()

    edited = st.data_editor(
        df_editable,
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "Hecho": st.column_config.CheckboxColumn("Hecho", help="Marcar como completado"),
        },
        disabled=[c for c in df_editable.columns if c != "Hecho"],  # solo editable "Hecho"
        key="admin_editor_hechos",
    )

    if st.button("💾 Guardar cambios", key="save_done"):
        # Alineamos índices (pueden venir como str)
        def _coerce_idx(idx):
            try:
                return idx.astype(int)
            except Exception:
                return idx

        orig_idx  = _coerce_idx(original_done.index)
        edit_idx  = _coerce_idx(edited.index)

        original_done.index = orig_idx
        edited_done = edited["Hecho"].astype(bool).copy()
        edited_done.index = edit_idx

        # Calculamos cambios donde ID esté en ambas series
        intersect_ids = [i for i in edited_done.index if i in original_done.index]
        cambios = [(int(i), bool(edited_done.loc[i]))
                   for i in intersect_ids
                   if bool(edited_done.loc[i]) != bool(original_done.loc[i])]

        if not cambios:
            st.info("No hay cambios que guardar.")
        else:
            who = (st.session_state.user or "admin").strip()
            when = datetime.utcnow().isoformat()
            with engine.begin() as conn:
                for vid, new_done in cambios:
                    conn.execute(
                        text("""
                            UPDATE vehicles
                            SET done = :d,
                                done_at = CASE WHEN :d THEN :dt ELSE done_at END,
                                done_by = CASE WHEN :d THEN :by ELSE done_by END
                            WHERE id = :id AND deleted_at IS NULL
                        """),
                        {"d": new_done, "dt": when, "by": who, "id": vid},
                    )
            st.success(f"Guardados {len(cambios)} cambio(s).")
            st.rerun()

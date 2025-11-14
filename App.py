# App.py - Control de vehículos

import os
from datetime import datetime, date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ============================================================
#  Helpers de consulta: listado por día + estilo "Hecho"
# ============================================================

def get_active_df(work_date_str: str, tipo: str | None = None) -> pd.DataFrame:
    """
    Devuelve los vehículos activos (no borrados) de una fecha concreta.
    Opcionalmente filtra por tipo (Turismo / Industrial).
    """
    base = """
        SELECT
            id         AS ID,
            modelo     AS Modelo,
            bastidor   AS Bastidor,
            color      AS Color,
            comercial  AS Comercial,
            hora_prevista AS "Hora prevista",
            matricula  AS Matrícula,
            comentarios AS Comentarios,
            placa      AS Placa,
            kit        AS Kit,
            alfombrillas AS Alfombrillas,
            kit_flota  AS "Kit flota",
            done       AS Hecho,
            tipo       AS Tipo,
            work_date  AS Fecha,
            created_at AS "Creado en (UTC)",
            created_by AS "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL
          AND work_date = %s
    """
    if tipo:
        query = base + " AND tipo = %s ORDER BY id DESC"
        params = (work_date_str, tipo)
    else:
        query = base + " ORDER BY id DESC"
        params = (work_date_str,)

    return pd.read_sql(query, engine, params=params)


def style_done(df: pd.DataFrame):
    """Pinta en verde las filas con Hecho = True."""
    if df.empty:
        return df
    # Buscamos la columna 'Hecho' sin liarnos con mayúsculas
    colmap = {c.lower(): c for c in df.columns}
    if "hecho" not in colmap:
        return df
    col_hecho = colmap["hecho"]

    def _row_style(row):
        done = bool(row.get(col_hecho, False))
        return ['background-color: #e8ffe8' if done else '' for _ in row]

    return df.style.apply(_row_style, axis=1)

# ============================================================
#  Configuración / Secrets
# ============================================================

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

MAX_PER_DAY = 15  # límite total por día (Turismo + Industrial)

# ============================================================
#  Conexión BD (SQLAlchemy)
# ============================================================

def get_engine() -> Engine:
    return create_engine(DATABASE_URL, pool_pre_ping=True)

engine = get_engine()

# ============================================================
#  DDL + Migraciones
# ============================================================

DDL_VEHICLES_PG = text(
    """
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
        kit_flota BOOLEAN DEFAULT FALSE,
        done BOOLEAN DEFAULT FALSE,
        done_at TEXT,
        done_by TEXT,
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
        hora_prevista TEXT,
        matricula TEXT,
        comentarios TEXT,
        work_date TEXT,
        tipo TEXT DEFAULT 'Turismo',
        placa BOOLEAN DEFAULT FALSE,
        kit BOOLEAN DEFAULT FALSE,
        alfombrillas BOOLEAN DEFAULT FALSE,
        kit_flota BOOLEAN DEFAULT FALSE,
        done BOOLEAN DEFAULT FALSE,
        done_at TEXT,
        done_by TEXT,
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
        # 1) Crear tablas según motor
        if is_sqlite:
            conn.execute(DDL_VEHICLES_SQLITE)
            conn.execute(DDL_ACCESS_SQLITE)
        else:
            conn.execute(DDL_VEHICLES_PG)
            conn.execute(DDL_ACCESS_PG)

        # 2) Migraciones idempotentes

        # work_date
        try:
            conn.execute(text("ALTER TABLE vehicles ADD COLUMN work_date TEXT"))
        except Exception:
            pass

        # tipo
        try:
            conn.execute(text("ALTER TABLE vehicles ADD COLUMN tipo TEXT"))
        except Exception:
            pass
        try:
            conn.execute(
                text("UPDATE vehicles SET tipo = 'Turismo' WHERE tipo IS NULL OR tipo = 'Comercial'")
            )
        except Exception:
            pass
        if not is_sqlite:
            try:
                conn.execute(text("ALTER TABLE vehicles ALTER COLUMN tipo SET DEFAULT 'Turismo'"))
            except Exception:
                pass

        # hora_prevista nullable (solo PG)
        if not is_sqlite:
            try:
                conn.execute(text("ALTER TABLE vehicles ALTER COLUMN hora_prevista DROP NOT NULL"))
            except Exception:
                pass

        # columnas booleanas nuevas
        for coldef in [
            "placa BOOLEAN DEFAULT FALSE",
            "kit BOOLEAN DEFAULT FALSE",
            "alfombrillas BOOLEAN DEFAULT FALSE",
            "kit_flota BOOLEAN DEFAULT FALSE",
            "done BOOLEAN DEFAULT FALSE",
            "done_at TEXT",
            "done_by TEXT",
        ]:
            try:
                conn.execute(text(f"ALTER TABLE vehicles ADD COLUMN {coldef}"))
            except Exception:
                pass

# ============================================================
#  Utilidades varias
# ============================================================

WEEKDAYS_ES = [
    "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"
]

def es_weekday_name(d: date) -> str:
    return WEEKDAYS_ES[d.weekday()]

def is_weekday(d: date) -> bool:
    return d.weekday() < 5  # 0..4 => Lun..Vie


def get_active_all_df(
    date_from: str | None = None,
    date_to: str | None = None,
    tipo: str | None = None,
) -> pd.DataFrame:
    """
    Usado en el panel admin.
    Devuelve coches NO HECHOS (done FALSE/NULL),
    con work_date <= hoy, opcionalmente filtrando por rango y tipo.
    """
    today_str = date.today().isoformat()
    base = """
        SELECT
            id         AS ID,
            modelo     AS Modelo,
            bastidor   AS Bastidor,
            color      AS Color,
            comercial  AS Comercial,
            hora_prevista AS "Hora prevista",
            matricula  AS Matrícula,
            comentarios AS Comentarios,
            placa      AS Placa,
            kit        AS Kit,
            alfombrillas AS Alfombrillas,
            kit_flota  AS "Kit flota",
            done       AS Hecho,
            tipo       AS Tipo,
            work_date  AS Fecha,
            created_at AS "Creado en (UTC)",
            created_by AS "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL
          AND (done IS NULL OR done = 0)
          AND work_date <= %s
    """
    conds = []
    params: list = [today_str]

    if date_from:
        conds.append("AND work_date >= %s")
        params.append(date_from)
    if date_to:
        conds.append("AND work_date <= %s")
        params.append(date_to)
    if tipo and tipo in ("Turismo", "Industrial"):
        conds.append("AND tipo = %s")
        params.append(tipo)

    order = " ORDER BY work_date DESC, id DESC"
    query = base + " ".join(conds) + order
    return pd.read_sql(query, engine, params=tuple(params))

# ============================================================
#  Operaciones de datos
# ============================================================

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
                    modelo, bastidor, color, comercial,
                    hora_prevista, matricula, comentarios,
                    work_date, tipo,
                    placa, kit, alfombrillas, kit_flota,
                    done, created_at, created_by
                ) VALUES (
                    :modelo, :bastidor, :color, :comercial,
                    :hora_prevista, :matricula, :comentarios,
                    :work_date, :tipo,
                    :placa, :kit, :alfombrillas, :kit_flota,
                    FALSE, :created_at, :created_by
                )
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
                "tipo": tipo or "Turismo",
                "placa": bool(data.get("placa")),
                "kit": bool(data.get("kit")),
                "alfombrillas": bool(data.get("alfombrillas")),
                "kit_flota": bool(data.get("kit_flota")),
                "created_at": datetime.utcnow().isoformat(),
                "created_by": user,
            },
        )


def get_active_count(work_date_str: str) -> int:
    with engine.begin() as conn:
        cnt = conn.execute(
            text(
                "SELECT COUNT(*) FROM vehicles "
                "WHERE deleted_at IS NULL AND work_date = :d"
            ),
            {"d": work_date_str},
        ).scalar()
        return int(cnt or 0)


def get_count_by_type(work_date_str: str, tipo: str) -> int:
    with engine.begin() as conn:
        cnt = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM vehicles
                WHERE deleted_at IS NULL AND work_date = :d AND tipo = :t
                """
            ),
            {"d": work_date_str, "t": tipo},
        ).scalar()
        return int(cnt or 0)


def get_all_df() -> pd.DataFrame:
    query = """
        SELECT
            id          AS ID,
            modelo      AS Modelo,
            bastidor    AS Bastidor,
            color       AS Color,
            comercial   AS Comercial,
            hora_prevista AS "Hora prevista",
            matricula   AS Matrícula,
            comentarios AS Comentarios,
            placa       AS Placa,
            kit         AS Kit,
            alfombrillas AS Alfombrillas,
            kit_flota   AS "Kit flota",
            done        AS Hecho,
            tipo        AS Tipo,
            work_date   AS Fecha,
            created_at  AS "Creado en (UTC)",
            created_by  AS "Creado por",
            deleted_at  AS "Borrado en (UTC)",
            deleted_by  AS "Borrado por",
            delete_reason AS "Motivo borrado"
        FROM vehicles
        ORDER BY id DESC
    """
    return pd.read_sql(query, engine)


def soft_delete_vehicle(vehicle_id: int, admin_user: str, reason: str):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE vehicles
                SET deleted_at = :d,
                    deleted_by = :u,
                    delete_reason = :r
                WHERE id = :id AND deleted_at IS NULL
                """
            ),
            {"d": datetime.utcnow().isoformat(), "u": admin_user, "r": reason.strip(), "id": vehicle_id},
        )

# ============================================================
#  UI (Streamlit)
# ============================================================

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

st.title("🚚 Control de vehículos por día (máx. 15 coches/día)")

# ---------------------------------
# PANTALLA 1 (PÚBLICA)
# ---------------------------------
if "user" not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.subheader("Identifícate")

    d_sel = selector_fecha_sidebar()
    work_date_str = d_sel.isoformat()

    st.markdown("### Vehículos activos del día seleccionado (público)")
    df_public_raw = get_active_df(work_date_str)

    if df_public_raw.empty:
        st.info("No hay vehículos activos para esta fecha.")
    else:
        # Ocultamos columnas ID y Creado en (UTC) en la vista pública
        cols_drop = ["ID", "Creado en (UTC)"]
        df_public = df_public_raw.drop(columns=cols_drop, errors="ignore")
        st.caption("La columna **Hecho** indica si el coche está terminado. (Filas verdes = hecho).")
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
# PANTALLA 2 (TRAS LOGIN)
# ---------------------------------
with st.sidebar:
    st.caption(f"Conectado como: **{st.session_state.user}**")
    if st.button("Cambiar de usuario"):
        st.session_state.user = None
        st.rerun()

d_sel = selector_fecha_sidebar()
work_date_str = d_sel.isoformat()
weekday_name = es_weekday_name(d_sel)

count_t = get_count_by_type(work_date_str, "Turismo")
count_i = get_count_by_type(work_date_str, "Industrial")
total_hoy = get_active_count(work_date_str)

st.write(
    f"Registros {weekday_name} {d_sel.strftime('%d-%m-%Y')}: "
    f"**Total {total_hoy}/{MAX_PER_DAY}** "
    f"(Turismo: {count_t}, Industriales: {count_i})"
)

# Tabs
pest_turismo, pest_industrial = st.tabs(["🚘 Turismo", "🚛 Industriales"])

# ------------- TURISMO -------------
with pest_turismo:
    total_hoy = get_active_count(work_date_str)
    disabled = (total_hoy >= MAX_PER_DAY) or (not is_weekday(d_sel))

    st.caption(f"Turismo activos hoy: {count_t}. Total día: {total_hoy}/{MAX_PER_DAY}")
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add_turismo", clear_on_submit=True):
        st.subheader("Añadir vehículo (Turismo)")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_t")
        bastidor = st.text_input("Bastidor (8 caracteres)", disabled=disabled, key="bas_t")
        color = st.text_input("Color", disabled=disabled, key="col_t")
        comercial_name = st.text_input("Comercial", disabled=disabled, key="com_t")
        hora_prevista = st.text_input(
            "Hora prevista", placeholder="Ej: 10:30 o 2025-11-02 10:30",
            disabled=disabled, key="hor_t"
        )
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled, key="mat_t")
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled, key="coments_t")

        placa = st.checkbox("Placa", value=False, disabled=disabled, key="placa_t")
        kit = st.checkbox("Kit", value=False, disabled=disabled, key="kit_t")
        alfombrillas = st.checkbox("Alfombrillas", value=False, disabled=disabled, key="alf_t")
        kit_flota = st.checkbox("Kit flota", value=False, disabled=disabled, key="kitflota_t")

        submitted = st.form_submit_button("Guardar (Turismo)", disabled=disabled)

        if submitted:
            bastidor_str = (bastidor or "").strip()
            if len(bastidor_str) != 8:
                st.error("El bastidor debe tener **exactamente 8 caracteres**.")
            else:
                campos = {
                    "modelo": modelo or "",
                    "bastidor": bastidor_str,
                    "color": color or "",
                    "comercial": comercial_name or "",
                    "hora_prevista": hora_prevista or "",
                    "matricula": matricula or "",
                    "comentarios": comentarios or "",
                    "placa": placa,
                    "kit": kit,
                    "alfombrillas": alfombrillas,
                    "kit_flota": kit_flota,
                }
                if not all(campos[k].strip() for k in ["modelo", "bastidor", "color", "comercial"]):
                    st.warning("Completa los campos obligatorios.")
                elif get_active_count(work_date_str) >= MAX_PER_DAY:
                    st.error("Límite de 15 coches alcanzado para esta fecha. No se guardó.")
                else:
                    insert_vehicle(campos, st.session_state.user, work_date_str, "Turismo")
                    st.success("Vehículo (Turismo) guardado.")
                    st.rerun()

    st.markdown("### Turismo activos en la fecha")
    df_t = get_active_df(work_date_str, "Turismo")
    if df_t.empty:
        st.info("No hay Turismo para esta fecha.")
    else:
        st.caption("Verde = 'Hecho'")
        st.dataframe(style_done(df_t), use_container_width=True, hide_index=True)

# ------------- INDUSTRIAL -------------
with pest_industrial:
    total_hoy = get_active_count(work_date_str)
    disabled = (total_hoy >= MAX_PER_DAY) or (not is_weekday(d_sel))

    st.caption(f"Industriales activos hoy: {count_i}. Total día: {total_hoy}/{MAX_PER_DAY}")
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")

    with st.form("form_add_industrial", clear_on_submit=True):
        st.subheader("Añadir vehículo industrial")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_i")
        bastidor = st.text_input("Bastidor (8 caracteres)", disabled=disabled, key="bas_i")
        color = st.text_input("Color", disabled=disabled, key="col_i")
        comercial_name = st.text_input("Comercial", disabled=disabled, key="com_i")
        hora_prevista = st.text_input(
            "Hora prevista", placeholder="Ej: 10:30 o 2025-11-02 10:30",
            disabled=disabled, key="hor_i"
        )
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled, key="mat_i")
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled, key="coments_i")

        placa_i = st.checkbox("Placa", value=False, disabled=disabled, key="placa_i")
        kit_i = st.checkbox("Kit", value=False, disabled=disabled, key="kit_i")
        alfombrillas_i = st.checkbox("Alfombrillas", value=False, disabled=disabled, key="alf_i")
        kit_flota_i = st.checkbox("Kit flota", value=False, disabled=disabled, key="kitflota_i")

        submitted = st.form_submit_button("Guardar (Industrial)", disabled=disabled)

        if submitted:
            bastidor_str = (bastidor or "").strip()
            if len(bastidor_str) != 8:
                st.error("El bastidor debe tener **exactamente 8 caracteres**.")
            else:
                campos = {
                    "modelo": modelo or "",
                    "bastidor": bastidor_str,
                    "color": color or "",
                    "comercial": comercial_name or "",
                    "hora_prevista": hora_prevista or "",
                    "matricula": matricula or "",
                    "comentarios": comentarios or "",
                    "placa": placa_i,
                    "kit": kit_i,
                    "alfombrillas": alfombrillas_i,
                    "kit_flota": kit_flota_i,
                }
                if not all(campos[k].strip() for k in ["modelo", "bastidor", "color", "comercial"]):
                    st.warning("Completa los campos obligatorios.")
                elif get_active_count(work_date_str) >= MAX_PER_DAY:
                    st.error("Límite de 15 coches alcanzado para esta fecha. No se guardó.")
                else:
                    insert_vehicle(campos, st.session_state.user, work_date_str, "Industrial")
                    st.success("Vehículo industrial guardado.")
                    st.rerun()

    st.markdown("### Industriales activos en la fecha")
    df_i = get_active_df(work_date_str, "Industrial")
    if df_i.empty:
        st.info("No hay Industriales para esta fecha.")
    else:
        st.caption("Verde = 'Hecho'")
        st.dataframe(style_done(df_i), use_container_width=True, hide_index=True)

# ============================================================
#  EXPORT / LOG DE ACCESOS / PANEL ADMIN
# ============================================================

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
                'SELECT id as ID, username as Usuario, accessed_at as "Accedido en (UTC)" '
                "FROM access_log ORDER BY id DESC",
                engine,
            )
            st.dataframe(access_df, use_container_width=True, hide_index=True)

st.divider()
with st.expander("🔐 Admin – marcar coches terminados (todos los días)"):
    admin_pass_admin = st.text_input("Contraseña de administrador", type="password", key="admin_panel_pwd")

    if admin_pass_admin != ADMIN_PASSWORD:
        if admin_pass_admin:
            st.error("Contraseña incorrecta.")
    else:
        st.success("Acceso concedido.")

        # Filtros
        colf1, colf2, colf3 = st.columns([1, 1, 1])
        with colf1:
            filtro_desde = st.checkbox("Filtrar desde")
            df_desde = st.date_input("Desde", value=date.today()) if filtro_desde else None
        with colf2:
            filtro_hasta = st.checkbox("Filtrar hasta")
            df_hasta = st.date_input("Hasta", value=date.today()) if filtro_hasta else None
        with colf3:
            tipo_sel = st.selectbox("Tipo", ["Todos", "Turismo", "Industrial"], index=0)

        date_from = df_desde.isoformat() if df_desde else None
        date_to = df_hasta.isoformat() if df_hasta else None
        tipo_arg = None if tipo_sel == "Todos" else tipo_sel

        df_admin = get_active_all_df(date_from=date_from, date_to=date_to, tipo=tipo_arg)

        if df_admin.empty:
            st.info("No hay coches pendientes (no hechos) con esos filtros.")
        else:
            st.caption("La columna **Hecho** indica si el coche está terminado. (Filas verdes = hecho).")
            st.dataframe(style_done(df_admin), use_container_width=True, hide_index=True)

            st.markdown("#### Editar estado 'Hecho' (Placa / Kit / Alfombrillas / Kit flota)")
            editable_cols = [
                c for c in
                ["ID", "Modelo", "Bastidor", "Hora prevista",
                 "Placa", "Kit", "Alfombrillas", "Kit flota",
                 "Tipo", "Fecha"]
                if c in df_admin.columns
            ]
            editable = df_admin[editable_cols].copy()

            edited = st.data_editor(
                editable,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "Placa":        st.column_config.CheckboxColumn("Placa"),
                    "Kit":          st.column_config.CheckboxColumn("Kit"),
                    "Alfombrillas": st.column_config.CheckboxColumn("Alfombrillas"),
                    "Kit flota":    st.column_config.CheckboxColumn("Kit flota"),
                },
                key="admin_editor_hechos",
            )

            if st.button("💾 Guardar cambios"):
                if "ID" not in editable.columns:
                    st.error("No se encontró la columna ID en los datos del administrador.")
                else:
                    original = editable.set_index("ID")
                    cambios = []

                    for _, row in edited.iterrows():
                        vid = int(row["ID"])
                        placa_new = bool(row.get("Placa", False))
                        kit_new = bool(row.get("Kit", False))
                        alf_new = bool(row.get("Alfombrillas", False))
                        kit_flota_new = bool(row.get("Kit flota", False))

                        # Estado anterior (para no tocar filas sin cambios)
                        placa_old = bool(original.loc[vid].get("Placa", False))
                        kit_old = bool(original.loc[vid].get("Kit", False))
                        alf_old = bool(original.loc[vid].get("Alfombrillas", False))
                        kit_flota_old = bool(original.loc[vid].get("Kit flota", False))

                        if (
                            placa_new != placa_old
                            or kit_new != kit_old
                            or alf_new != alf_old
                            or kit_flota_new != kit_flota_old
                        ):
                            done_new = placa_new and kit_new and alf_new and kit_flota_new
                            cambios.append(
                                (vid, placa_new, kit_new, alf_new, kit_flota_new, done_new)
                            )

                    if not cambios:
                        st.info("No hay cambios que guardar.")
                    else:
                        who = (st.session_state.user or "admin").strip()
                        when = datetime.utcnow().isoformat()
                        with engine.begin() as conn:
                            for vid, placa_new, kit_new, alf_new, kit_flota_new, done_new in cambios:
                                conn.execute(
                                    text(
                                        """
                                        UPDATE vehicles
                                        SET placa = :placa,
                                            kit = :kit,
                                            alfombrillas = :alf,
                                            kit_flota = :kitflota,
                                            done = :done,
                                            done_at = CASE WHEN :done THEN :dt ELSE done_at END,
                                            done_by = CASE WHEN :done THEN :by ELSE done_by END
                                        WHERE id = :id AND deleted_at IS NULL
                                        """
                                    ),
                                    {
                                        "placa": placa_new,
                                        "kit": kit_new,
                                        "alf": alf_new,
                                        "kitflota": kit_flota_new,
                                        "done": done_new,
                                        "dt": when,
                                        "by": who,
                                        "id": vid,
                                    },
                                )
                        st.success(f"Guardados {len(cambios)} cambio(s).")
                        st.rerun()

st.caption(
    "Hecho con ❤️ en Streamlit + SQLAlchemy. "
    "Listado público por fecha, límite 15 coches/día, panel admin con estado de montaje."
)

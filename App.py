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

# Máximo total (Turismo + Industrial) por día
MAX_PER_DAY = 15

# =========================
# Conexión BD
# =========================

def get_engine() -> Engine:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return engine

engine = get_engine()

# =========================
# DDL tablas
# =========================

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

# =========================
# Init DB + migraciones
# =========================

def init_db():
    is_sqlite = DATABASE_URL.startswith("sqlite:")
    with engine.begin() as conn:
        # Crear tablas base
        if is_sqlite:
            conn.execute(DDL_VEHICLES_SQLITE)
            conn.execute(DDL_ACCESS_SQLITE)
        else:
            conn.execute(DDL_VEHICLES_PG)
            conn.execute(DDL_ACCESS_PG)

        # work_date (por si faltara)
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

        # hora_prevista nullable (solo Postgres)
        if not is_sqlite:
            try:
                conn.execute(text("ALTER TABLE vehicles ALTER COLUMN hora_prevista DROP NOT NULL"))
            except Exception:
                pass

        # columnas nuevas
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

# =========================
# Utilidades generales
# =========================

WEEKDAYS_ES = [
    "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"
]

def es_weekday_name(d: date) -> str:
    return WEEKDAYS_ES[d.weekday()]

def is_weekday(d: date) -> bool:
    return d.weekday() < 5  # Lun–Vie

# =========================
# Consultas / Datos
# =========================

def get_count_by_type(work_date_str: str, tipo: str) -> int:
    query = (
        "SELECT COUNT(*) AS c FROM vehicles "
        f"WHERE deleted_at IS NULL AND work_date = '{work_date_str}' AND tipo = '{tipo}'"
    )
    with engine.begin() as conn:
        res = conn.execute(text(query)).scalar()
    return int(res or 0)

def get_active_count(work_date_str: str) -> int:
    query = (
        "SELECT COUNT(*) AS c FROM vehicles "
        f"WHERE deleted_at IS NULL AND work_date = '{work_date_str}'"
    )
    with engine.begin() as conn:
        res = conn.execute(text(query)).scalar()
    return int(res or 0)

def get_active_df(work_date_str: str, tipo: str | None = None) -> pd.DataFrame:
    base = """
        SELECT id AS ID,
               modelo AS Modelo,
               bastidor AS Bastidor,
               color AS Color,
               comercial AS Comercial,
               hora_prevista AS "Hora prevista",
               matricula AS Matrícula,
               comentarios AS Comentarios,
               placa AS Placa,
               kit AS Kit,
               alfombrillas AS Alfombrillas,
               done AS Hecho,
               tipo AS Tipo,
               work_date AS Fecha,
               created_at AS "Creado en",
               created_by AS "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL
          AND work_date = '{d}'
    """.format(d=work_date_str)

    if tipo:
        base += " AND tipo = '{t}'".format(t=tipo)

    base += " ORDER BY id DESC"

    return pd.read_sql(text(base), engine)

def get_active_all_df(date_from: str | None = None,
                      date_to: str | None = None,
                      tipo: str | None = None) -> pd.DataFrame:
    base = """
        SELECT id AS ID,
               modelo AS Modelo,
               bastidor AS Bastidor,
               color AS Color,
               comercial AS Comercial,
               hora_prevista AS "Hora prevista",
               matricula AS Matrícula,
               comentarios AS Comentarios,
               placa AS Placa,
               kit AS Kit,
               alfombrillas AS Alfombrillas,
               done AS Hecho,
               tipo AS Tipo,
               work_date AS Fecha,
               created_at AS "Creado en",
               created_by AS "Creado por"
        FROM vehicles
        WHERE deleted_at IS NULL
    """
    conds = []
    if date_from:
        conds.append(f"work_date >= '{date_from}'")
    if date_to:
        conds.append(f"work_date <= '{date_to}'")
    if tipo and tipo in ("Turismo", "Industrial"):
        conds.append(f"tipo = '{tipo}'")

    if conds:
        base += " AND " + " AND ".join(conds)

    base += " ORDER BY work_date DESC, id DESC"

    return pd.read_sql(text(base), engine)

def get_all_df() -> pd.DataFrame:
    query = """
        SELECT id AS ID,
               modelo AS Modelo,
               bastidor AS Bastidor,
               color AS Color,
               comercial AS Comercial,
               hora_prevista AS "Hora prevista",
               matricula AS Matrícula,
               comentarios AS Comentarios,
               tipo AS Tipo,
               work_date AS Fecha,
               created_at AS "Creado en",
               created_by AS "Creado por",
               deleted_at AS "Borrado en",
               deleted_by AS "Borrado por",
               delete_reason AS "Motivo borrado"
        FROM vehicles
        ORDER BY id DESC
    """
    return pd.read_sql(text(query), engine)

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
                    placa, kit, alfombrillas, done,
                    created_at, created_by
                ) VALUES (
                    :modelo, :bastidor, :color, :comercial,
                    :hora_prevista, :matricula, :comentarios,
                    :work_date, :tipo,
                    :placa, :kit, :alfombrillas, FALSE,
                    :created_at, :created_by
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
                "created_at": datetime.utcnow().isoformat(),
                "created_by": user,
            },
        )

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
            {
                "d": datetime.utcnow().isoformat(),
                "u": admin_user,
                "r": reason.strip(),
                "id": vehicle_id,
            },
        )

def update_done_flags(changes: list[tuple[int, bool, bool, bool]] , who: str):
    """
    changes: lista de tuplas (id, placa, kit, alfombrillas)
    done = True solo si las 3 están a True
    """
    when = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        for vid, placa, kit, alf in changes:
            done = bool(placa and kit and alf)
            conn.execute(
                text(
                    """
                    UPDATE vehicles
                    SET placa = :placa,
                        kit = :kit,
                        alfombrillas = :alf,
                        done = :done,
                        done_at = CASE WHEN :done THEN :dt ELSE done_at END,
                        done_by = CASE WHEN :done THEN :by ELSE done_by END
                    WHERE id = :id AND deleted_at IS NULL
                    """
                ),
                {
                    "placa": bool(placa),
                    "kit": bool(kit),
                    "alf": bool(alf),
                    "done": done,
                    "dt": when,
                    "by": who,
                    "id": vid,
                },
            )

# =========================
# Estilo filas Hecho = True
# =========================

def style_done(df: pd.DataFrame):
    if df.empty:
        return df
    colmap = {c.lower(): c for c in df.columns}
    if "hecho" not in colmap:
        return df
    col_hecho = colmap["hecho"]

    def _row_style(row):
        try:
            done = bool(row[col_hecho])
        except Exception:
            done = False
        return ['background-color: #e8ffe8' if done else '' for _ in row]

    return df.style.apply(_row_style, axis=1)

# =========================
# UI (Streamlit)
# =========================

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

st.title("🚚 Control de vehículos por día (máx. 15 activos en total)")

# -------------------------
# Login sencillo
# -------------------------
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
        # Quitamos ID y Creado en para público
        cols = [c for c in df_public_raw.columns if c not in ("ID", "Creado en")]
        df_public = df_public_raw[cols]
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

# -------------------------
# Pantalla tras login
# -------------------------
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
total = count_t + count_i

st.write(
    f"Registros {weekday_name} {d_sel.strftime('%d-%m-%Y')}: "
    f"**Turismo {count_t}** · "
    f"**Industriales {count_i}** · "
    f"**Total {total}/{MAX_PER_DAY}**"
)

pest_turismo, pest_industrial = st.tabs(["🚘 Turismo", "🚛 Industriales"])

# -------------------------
# Formulario Turismo
# -------------------------
with pest_turismo:
    disabled = (total >= MAX_PER_DAY) or (not is_weekday(d_sel))
    st.caption(f"Turismos activos hoy: {count_t}")
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")
    elif total >= MAX_PER_DAY:
        st.error(f"Límite total de {MAX_PER_DAY} vehículos alcanzado para esta fecha.")

    with st.form("form_add_turismo", clear_on_submit=True):
        st.subheader("Añadir vehículo (Turismo)")
        modelo = st.text_input("Modelo", disabled=disabled)
        bastidor = st.text_input("Bastidor (8 caracteres)", disabled=disabled)
        color = st.text_input("Color", disabled=disabled)
        comercial_name = st.text_input("Comercial", disabled=disabled)
        hora_prevista = st.text_input(
            "Hora prevista (opcional)",
            placeholder="Ej: 10:30 o 2025-11-02 10:30",
            disabled=disabled,
        )
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled)
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled)

        placa = st.checkbox("Placa", value=False, disabled=disabled)
        kit = st.checkbox("Kit", value=False, disabled=disabled)
        alfombrillas = st.checkbox("Alfombrillas", value=False, disabled=disabled)

        submitted = st.form_submit_button("Guardar (Turismo)", disabled=disabled)

        if submitted:
            # Validaciones
            campos_oblig = {
                "modelo": modelo or "",
                "bastidor": bastidor or "",
                "color": color or "",
                "comercial": comercial_name or "",
            }
            if not all(v.strip() for v in campos_oblig.values()):
                st.warning("Completa los campos obligatorios.")
                st.stop()
            if len((bastidor or "").strip()) != 8:
                st.error("El bastidor debe tener exactamente 8 caracteres.")
                st.stop()
            if get_active_count(work_date_str) >= MAX_PER_DAY:
                st.error("Límite total alcanzado para esta fecha. No se guardó.")
                st.stop()

            campos = {
                **campos_oblig,
                "hora_prevista": hora_prevista or "",
                "matricula": matricula or "",
                "comentarios": comentarios or "",
                "placa": placa,
                "kit": kit,
                "alfombrillas": alfombrillas,
            }
            insert_vehicle(campos, st.session_state.user, work_date_str, "Turismo")
            st.success("Vehículo (Turismo) guardado.")
            st.rerun()

    st.markdown("### Turismos activos en la fecha")
    df_t = get_active_df(work_date_str, "Turismo")
    if df_t.empty:
        st.info("No hay turismos para esta fecha.")
    else:
        st.caption("Verde = Hecho")
        st.dataframe(style_done(df_t), use_container_width=True, hide_index=True)

# -------------------------
# Formulario Industrial
# -------------------------
with pest_industrial:
    disabled = (total >= MAX_PER_DAY) or (not is_weekday(d_sel))
    st.caption(f"Industriales activos hoy: {count_i}")
    if not is_weekday(d_sel):
        st.warning("Selecciona una fecha de lunes a viernes para habilitar el formulario.")
    elif total >= MAX_PER_DAY:
        st.error(f"Límite total de {MAX_PER_DAY} vehículos alcanzado para esta fecha.")

    with st.form("form_add_industrial", clear_on_submit=True):
        st.subheader("Añadir vehículo (Industrial)")
        modelo = st.text_input("Modelo", disabled=disabled, key="mod_i")
        bastidor = st.text_input("Bastidor (8 caracteres)", disabled=disabled, key="bas_i")
        color = st.text_input("Color", disabled=disabled, key="col_i")
        comercial_name = st.text_input("Comercial", disabled=disabled, key="com_i")
        hora_prevista = st.text_input(
            "Hora prevista (opcional)",
            placeholder="Ej: 10:30 o 2025-11-02 10:30",
            disabled=disabled,
            key="hor_i",
        )
        matricula = st.text_input("Matrícula (opcional)", disabled=disabled, key="mat_i")
        comentarios = st.text_area("Comentarios (opcional)", disabled=disabled, key="coments_i")

        placa_i = st.checkbox("Placa", value=False, disabled=disabled, key="placa_i")
        kit_i = st.checkbox("Kit", value=False, disabled=disabled, key="kit_i")
        alfombrillas_i = st.checkbox("Alfombrillas", value=False, disabled=disabled, key="alf_i")

        submitted = st.form_submit_button("Guardar (Industrial)", disabled=disabled)

        if submitted:
            campos_oblig = {
                "modelo": modelo or "",
                "bastidor": bastidor or "",
                "color": color or "",
                "comercial": comercial_name or "",
            }
            if not all(v.strip() for v in campos_oblig.values()):
                st.warning("Completa los campos obligatorios.")
                st.stop()
            if len((bastidor or "").strip()) != 8:
                st.error("El bastidor debe tener exactamente 8 caracteres.")
                st.stop()
            if get_active_count(work_date_str) >= MAX_PER_DAY:
                st.error("Límite total alcanzado para esta fecha. No se guardó.")
                st.stop()

            campos = {
                **campos_oblig,
                "hora_prevista": hora_prevista or "",
                "matricula": matricula or "",
                "comentarios": comentarios or "",
                "placa": placa_i,
                "kit": kit_i,
                "alfombrillas": alfombrillas_i,
            }
            insert_vehicle(campos, st.session_state.user, work_date_str, "Industrial")
            st.success("Vehículo industrial guardado.")
            st.rerun()

    st.markdown("### Industriales activos en la fecha")
    df_i = get_active_df(work_date_str, "Industrial")
    if df_i.empty:
        st.info("No hay industriales para esta fecha.")
    else:
        st.caption("Verde = Hecho")
        st.dataframe(style_done(df_i), use_container_width=True, hide_index=True)

st.divider()

# =========================
# Exportación
# =========================
st.sidebar.divider()
st.sidebar.markdown("**Exportar**")
all_df = get_all_df()
st.sidebar.download_button(
    label="Descargar CSV (todo el histórico)",
    data=all_df.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"vehiculos_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)

# =========================
# Registro de accesos
# =========================
st.divider()
with st.expander("📜 Ver registro de accesos (requiere contraseña)"):
    admin_pass_log = st.text_input("Contraseña de administrador", type="password", key="access_log_pwd")
    if st.button("Ver registro", key="btn_access_log"):
        if admin_pass_log != ADMIN_PASSWORD:
            st.error("Contraseña incorrecta.")
        else:
            access_df = pd.read_sql(
                'SELECT id AS ID, username AS Usuario, accessed_at AS "Accedido en (UTC)" FROM access_log ORDER BY id DESC',
                engine,
            )
            st.dataframe(access_df, use_container_width=True, hide_index=True)

# =========================
# Admin – Kit Flota / Hecho
# =========================
st.divider()
with st.expander("🔐 Admin – Kit Flota / marcar coches terminados"):
    admin_pass_admin = st.text_input("Contraseña de administrador", type="password", key="admin_panel_pwd")

    if admin_pass_admin != ADMIN_PASSWORD:
        if admin_pass_admin:
            st.error("Contraseña incorrecta.")
    else:
        st.success("Acceso concedido.")

        colf1, colf2, colf3 = st.columns([1, 1, 1])
        with colf1:
            df_desde = st.date_input("Desde (opcional)", value=None)
        with colf2:
            df_hasta = st.date_input("Hasta (opcional)", value=None)
        with colf3:
            tipo_sel = st.selectbox("Tipo", ["Todos", "Turismo", "Industrial"], index=0)

        date_from = df_desde.isoformat() if df_desde else None
        date_to = df_hasta.isoformat() if df_hasta else None
        tipo_arg = None if tipo_sel == "Todos" else tipo_sel

        df_admin = get_active_all_df(date_from=date_from, date_to=date_to, tipo=tipo_arg)

        if df_admin.empty:
            st.info("No hay coches activos con esos filtros.")
        else:
            # Resumen Kit Flota
            st.subheader("Kit Flota (resumen)")
            total_placa = int(df_admin["Placa"].sum())
            total_kit = int(df_admin["Kit"].sum())
            total_alf = int(df_admin["Alfombrillas"].sum())
            c1, c2, c3 = st.columns(3)
            c1.metric("Placas puestas", total_placa)
            c2.metric("Kit puesto", total_kit)
            c3.metric("Alfombrillas puestas", total_alf)

            st.caption("Verde = Hecho (las 3 casillas activadas).")
            st.dataframe(style_done(df_admin), use_container_width=True, hide_index=True)

            st.markdown("#### Editar Placa / Kit / Alfombrillas")
            editable_cols = [c for c in ["ID", "Modelo", "Bastidor", "Fecha", "Tipo", "Placa", "Kit", "Alfombrillas"] if c in df_admin.columns]
            editable = df_admin[editable_cols].copy()

            edited = st.data_editor(
                editable,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "Placa": st.column_config.CheckboxColumn("Placa"),
                    "Kit": st.column_config.CheckboxColumn("Kit"),
                    "Alfombrillas": st.column_config.CheckboxColumn("Alfombrillas"),
                },
                key="admin_editor_hechos",
            )

            if st.button("💾 Guardar cambios"):
                if "ID" not in editable.columns:
                    st.error("No se encuentra la columna ID en los datos.")
                else:
                    original = editable.set_index("ID")
                    cambios = []
                    for _, row in edited.iterrows():
                        vid = int(row["ID"])
                        placa = bool(row.get("Placa", False))
                        kit = bool(row.get("Kit", False))
                        alf = bool(row.get("Alfombrillas", False))

                        o_placa = bool(original.loc[vid, "Placa"])
                        o_kit = bool(original.loc[vid, "Kit"])
                        o_alf = bool(original.loc[vid, "Alfombrillas"])

                        if (placa, kit, alf) != (o_placa, o_kit, o_alf):
                            cambios.append((vid, placa, kit, alf))

                    if not cambios:
                        st.info("No hay cambios que guardar.")
                    else:
                        who = (st.session_state.user or "admin").strip()
                        update_done_flags(cambios, who)
                        st.success(f"Guardados {len(cambios)} cambio(s).")
                        st.rerun()

st.caption("Hecho con ❤️ en Streamlit + SQLAlchemy + Neon.")

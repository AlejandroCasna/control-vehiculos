from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql+psycopg2://neondb_owner:npg_uoQIlP6dqg4L@ep-wild-heart-aga6trro-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS access_log (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            accessed_at TEXT NOT NULL
        );
    """))
    conn.execute(text("""
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
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            deleted_at TEXT,
            deleted_by TEXT,
            delete_reason TEXT
        );
    """))
print("✅ Tablas creadas correctamente.")

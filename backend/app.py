import os
from datetime import datetime
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text

# --- DB config from env (matches docker-compose) ---
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rootpass")
DB_NAME = os.getenv("DB_NAME", "people_db")

DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = Flask(__name__)
engine = create_engine(DB_URL, pool_pre_ping=True)

# ---------- Helpers ----------
def row_to_dict(row_mapping):
    """Convert SQLAlchemy RowMapping to plain dict and make datetime JSON-safe."""
    d = dict(row_mapping)
    if isinstance(d.get("created_at"), datetime):
        d["created_at"] = d["created_at"].isoformat()
    return d

# ---------- Health ----------
@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}, 200
    except Exception as e:
        return {"status": "error", "detail": str(e)}, 500

# ---------- Create ----------
@app.post("/api/people")
def create_person():
    data = request.get_json(force=True, silent=True) or {}

    name = (data.get("name") or "").strip()
    age = data.get("age")
    gender = (data.get("gender") or "").strip()
    contact = (data.get("contact") or "").strip()

    # Basic validations
    if (
        not name
        or not isinstance(age, int) or age <= 0
        or gender not in ("Male", "Female", "Other")
        or not contact
    ):
        return jsonify({
            "error": "Invalid payload. Need name(str), age(int>0), gender(Male/Female/Other), contact(str)."
        }), 400

    sql = text("""
        INSERT INTO person (name, age, gender, contact)
        VALUES (:name, :age, :gender, :contact)
    """)
    with engine.begin() as conn:
        res = conn.execute(sql, {
            "name": name, "age": age, "gender": gender, "contact": contact
        })
        new_id = res.lastrowid

        # Return the created row
        row = conn.execute(
            text("SELECT id, name, age, gender, contact, created_at FROM person WHERE id=:id"),
            {"id": new_id}
        ).mappings().first()

    return jsonify(row_to_dict(row)), 201

# ---------- List ----------
@app.get("/api/people")
def list_people():
    sql = text("SELECT id, name, age, gender, contact, created_at FROM person ORDER BY id DESC")
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    out = [row_to_dict(r) for r in rows]
    return jsonify(out), 200

# ---------- Get by ID ----------
@app.get("/api/people/<int:pid>")
def get_person(pid):
    sql = text("SELECT id, name, age, gender, contact, created_at FROM person WHERE id=:id")
    with engine.connect() as conn:
        row = conn.execute(sql, {"id": pid}).mappings().first()

    if not row:
        return jsonify({"error": "Not found"}), 404

    return jsonify(row_to_dict(row)), 200

# ---------- Main ----------
if __name__ == "__main__":
    # Flask listens on all interfaces inside the container
    app.run(host="0.0.0.0", port=5000)

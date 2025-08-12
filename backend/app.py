import os
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rootpass")
DB_NAME = os.getenv("DB_NAME", "people_db")

DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = Flask(__name__)
engine = create_engine(DB_URL, pool_pre_ping=True)

@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}, 200
    except Exception as e:
        return {"status": "error", "detail": str(e)}, 500

@app.post("/api/people")
def create_person():
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip()
    age = data.get("age")
    gender = (data.get("gender") or "").strip()
    contact = (data.get("contact") or "").strip()

    if not name or not isinstance(age, int) or age <= 0 \
       or gender not in ("Male", "Female", "Other") or not contact:
        return jsonify({"error": "Invalid payload. Need name(str), age(int>0), gender(Male/Female/Other), contact(str)."}), 400

    sql = text("""INSERT INTO person (name, age, gender, contact)
                  VALUES (:name, :age, :gender, :contact)""")
    with engine.begin() as conn:
        res = conn.execute(sql, {"name": name, "age": age, "gender": gender, "contact": contact})
        new_id = res.lastrowid

    return jsonify({"id": new_id, "message": "Person created"}), 201

@app.get("/api/people")
def list_people():
    sql = text("SELECT id, name, age, gender, contact, created_at FROM person ORDER BY id DESC")
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return jsonify(list(rows)), 200

@app.get("/api/people/<int:pid>")
def get_person(pid):
    sql = text("SELECT id, name, age, gender, contact, created_at FROM person WHERE id=:id")
    with engine.connect() as conn:
        row = conn.execute(sql, {"id": pid}).mappings().first()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify(dict(row)), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

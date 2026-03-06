from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Body, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
import json
from typing import Any, Dict, Optional

from db import get_db, engine
from models import Base, Farm, Boundary, Lulc

Base.metadata.create_all(bind=engine)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS contributors (
            name TEXT PRIMARY KEY,
            passkey TEXT NOT NULL
        )
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pending_farm_edits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            farm_id INTEGER NOT NULL,
            barangay TEXT NOT NULL,
            contributor_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            old_props_json TEXT NOT NULL,
            new_props_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP NULL,
            reviewed_by TEXT NULL
        )
    """))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = "admin123"


def require_api_key(x_api_key: str = Header(default=None, alias="X-API-Key")):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


VALID_BARANGAYS = {"Poblacion", "Minsalirac", "San Isidro"}


def ensure_barangay(barangay: str):
    if barangay not in VALID_BARANGAYS:
        raise HTTPException(400, f"Unknown barangay: {barangay}")


def validate_feature_collection(obj: Any):
    if not isinstance(obj, dict):
        raise HTTPException(400, "Invalid GeoJSON: not a JSON object.")
    if obj.get("type") != "FeatureCollection":
        raise HTTPException(400, "Invalid GeoJSON: must be a FeatureCollection.")
    if "features" not in obj or not isinstance(obj["features"], list):
        raise HTTPException(400, "Invalid GeoJSON: 'features' must be a list.")


def _fc_from_rows(rows):
    features = []
    for r in rows:
        geom = json.loads(r.geom_geojson)
        props = json.loads(r.props_json)
        props["id"] = r.farm_id
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": geom
        })
    return {"type": "FeatureCollection", "features": features}


def contributor_ok(db: Session, name: str, key: str) -> bool:
    row = db.execute(
        text("SELECT passkey FROM contributors WHERE name = :n"),
        {"n": name}
    ).fetchone()
    if not row:
        return False
    return row[0] == key


@app.post("/api/contributors/register")
def register_contributor(
    payload: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    name = str(payload.get("name", "")).strip()
    passkey = str(payload.get("passkey", "")).strip()

    if not name or not passkey:
        raise HTTPException(400, "Missing name or passkey.")

    db.execute(
        text("""
            INSERT INTO contributors(name, passkey)
            VALUES(:n, :p)
            ON CONFLICT(name) DO UPDATE SET passkey = excluded.passkey
        """),
        {"n": name, "p": passkey}
    )
    db.commit()
    return {"ok": True, "message": f"Contributor '{name}' registered/updated."}


@app.get("/api/farms")
def get_farms(barangay: str, db: Session = Depends(get_db)):
    ensure_barangay(barangay)
    rows = db.query(Farm).filter(Farm.barangay == barangay).all()
    return _fc_from_rows(rows)


@app.get("/api/boundary")
def get_boundary(barangay: str, db: Session = Depends(get_db)):
    ensure_barangay(barangay)
    rows = db.query(Boundary).filter(Boundary.barangay == barangay).all()

    features = []
    for r in rows:
        geom = json.loads(r.geom_geojson)
        props = json.loads(r.props_json)
        features.append({"type": "Feature", "properties": props, "geometry": geom})
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/lulc")
def get_lulc(barangay: str, db: Session = Depends(get_db)):
    ensure_barangay(barangay)
    rows = db.query(Lulc).filter(Lulc.barangay == barangay).all()

    features = []
    for r in rows:
        geom = json.loads(r.geom_geojson)
        props = json.loads(r.props_json)
        features.append({"type": "Feature", "properties": props, "geometry": geom})
    return {"type": "FeatureCollection", "features": features}


@app.post("/api/upload-boundary")
async def upload_boundary(
    barangay: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    ensure_barangay(barangay)

    raw = await file.read()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Could not read JSON. Ensure it is valid UTF-8 GeoJSON.")

    validate_feature_collection(obj)
    if len(obj["features"]) < 1:
        raise HTTPException(400, "Boundary GeoJSON has no features.")

    db.query(Boundary).filter(Boundary.barangay == barangay).delete()

    for f in obj["features"]:
        geom = f.get("geometry")
        props = f.get("properties", {}) or {}
        if geom is None:
            raise HTTPException(400, "Invalid GeoJSON: boundary feature missing geometry.")
        db.add(Boundary(
            barangay=barangay,
            geom_geojson=json.dumps(geom, ensure_ascii=False),
            props_json=json.dumps(props, ensure_ascii=False),
        ))

    db.commit()
    return {"ok": True, "message": f"Boundary uploaded for {barangay}."}


@app.post("/api/upload-lulc")
async def upload_lulc(
    barangay: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    ensure_barangay(barangay)

    raw = await file.read()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Could not read JSON. Ensure it is valid UTF-8 GeoJSON.")

    validate_feature_collection(obj)
    if len(obj["features"]) < 1:
        raise HTTPException(400, "LULC GeoJSON has no features.")

    db.query(Lulc).filter(Lulc.barangay == barangay).delete()

    for f in obj["features"]:
        geom = f.get("geometry")
        props = f.get("properties", {}) or {}
        if geom is None:
            raise HTTPException(400, "Invalid GeoJSON: LULC feature missing geometry.")
        db.add(Lulc(
            barangay=barangay,
            geom_geojson=json.dumps(geom, ensure_ascii=False),
            props_json=json.dumps(props, ensure_ascii=False),
        ))

    db.commit()
    return {"ok": True, "message": f"LULC uploaded for {barangay}."}


@app.post("/api/upload-farms")
async def upload_farms(
    barangay: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    ensure_barangay(barangay)

    raw = await file.read()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Could not read JSON. Ensure it is valid UTF-8 GeoJSON.")

    validate_feature_collection(obj)
    if len(obj["features"]) < 1:
        raise HTTPException(400, "Farm GeoJSON has no features.")

    db.query(Farm).filter(Farm.barangay == barangay).delete()
    db.commit()

    next_id = 1
    for f in obj["features"]:
        if f.get("type") != "Feature":
            raise HTTPException(400, "Invalid GeoJSON: all items must be Feature objects.")
        if f.get("geometry") is None:
            raise HTTPException(400, "Invalid GeoJSON: each feature must have geometry.")

        props = f.get("properties", {}) or {}
        fid = props.get("id")

        if fid is None:
            props["id"] = next_id
            next_id += 1
        else:
            try:
                props["id"] = int(fid)
            except Exception:
                raise HTTPException(400, "Invalid GeoJSON: properties.id must be an integer.")
            next_id = max(next_id, props["id"] + 1)

        props.pop("ndvi_last", None)
        props.pop("ndvi_peak", None)
        props.pop("ndvi_drop", None)
        props.pop("status", None)

        f["properties"] = props

    for f in obj["features"]:
        geom = f["geometry"]
        props = f.get("properties", {}) or {}
        farm_id = int(props["id"])

        db.add(Farm(
            farm_id=farm_id,
            barangay=barangay,
            geom_geojson=json.dumps(geom, ensure_ascii=False),
            props_json=json.dumps(props, ensure_ascii=False),
        ))

    db.commit()
    return {"ok": True, "message": f"Farms uploaded for {barangay}."}


@app.put("/api/farms/{farm_id}")
def update_farm(
    barangay: str,
    farm_id: int,
    payload: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    x_contrib_name: Optional[str] = Header(default=None, alias="X-Contrib-Name"),
    x_contrib_key: Optional[str] = Header(default=None, alias="X-Contrib-Key"),
):
    ensure_barangay(barangay)

    row = db.query(Farm).filter(
        Farm.barangay == barangay,
        Farm.farm_id == farm_id
    ).first()

    if not row:
        raise HTTPException(404, "Farm not found")

    is_admin = (x_api_key == API_KEY)
    is_contrib = (
        (not is_admin)
        and x_contrib_name
        and x_contrib_key
        and contributor_ok(db, x_contrib_name, x_contrib_key)
    )

    if not (is_admin or is_contrib):
        raise HTTPException(401, "Unauthorized")

    stored_props = json.loads(row.props_json)

    if is_contrib:
        owner = str(stored_props.get("farmer", "")).strip()
        me = str(x_contrib_name).strip()

        if owner != me:
            raise HTTPException(403, "Not allowed to edit this farm.")

        if payload.get("type") == "Feature" and payload.get("geometry") is not None:
            raise HTTPException(403, "Contributors cannot edit geometry (map).")

    geom: Optional[Dict[str, Any]] = None
    props_in: Dict[str, Any] = {}

    if payload.get("type") == "Feature":
        geom = payload.get("geometry")
        props_in = payload.get("properties", {}) or {}
    else:
        props_in = payload

    new_props = dict(stored_props)

    for k, v in props_in.items():
        if k == "id":
            continue
        if is_contrib and k == "farmer":
            continue
        new_props[k] = v

    if is_contrib:
        new_props["farmer"] = stored_props.get("farmer", "")

    new_props["id"] = farm_id

    if is_admin:
        row.props_json = json.dumps(new_props, ensure_ascii=False)

        if geom is not None:
            row.geom_geojson = json.dumps(geom, ensure_ascii=False)

        db.commit()
        return {"ok": True, "mode": "direct", "message": "Farm updated by admin."}

    existing_pending = db.execute(text("""
        SELECT id
        FROM pending_farm_edits
        WHERE farm_id = :farm_id
          AND barangay = :barangay
          AND contributor_name = :contributor_name
          AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1
    """), {
        "farm_id": farm_id,
        "barangay": barangay,
        "contributor_name": x_contrib_name
    }).fetchone()

    if existing_pending:
        db.execute(text("""
            UPDATE pending_farm_edits
            SET old_props_json = :old_props_json,
                new_props_json = :new_props_json,
                created_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """), {
            "id": existing_pending[0],
            "old_props_json": json.dumps(stored_props, ensure_ascii=False),
            "new_props_json": json.dumps(new_props, ensure_ascii=False),
        })
    else:
        db.execute(text("""
            INSERT INTO pending_farm_edits (
                farm_id, barangay, contributor_name, status,
                old_props_json, new_props_json
            )
            VALUES (
                :farm_id, :barangay, :contributor_name, 'pending',
                :old_props_json, :new_props_json
            )
        """), {
            "farm_id": farm_id,
            "barangay": barangay,
            "contributor_name": x_contrib_name,
            "old_props_json": json.dumps(stored_props, ensure_ascii=False),
            "new_props_json": json.dumps(new_props, ensure_ascii=False),
        })

    db.commit()
    return {
        "ok": True,
        "mode": "pending",
        "message": "Edit submitted for admin approval."
    }


@app.delete("/api/farms/{farm_id}")
def delete_farm(
    barangay: str,
    farm_id: int,
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    ensure_barangay(barangay)

    row = db.query(Farm).filter(
        Farm.barangay == barangay,
        Farm.farm_id == farm_id
    ).first()

    if not row:
        raise HTTPException(404, "Farm not found")

    db.delete(row)
    db.commit()
    return {"ok": True}


@app.get("/api/pending-edits")
def list_pending_edits(
    barangay: Optional[str] = None,
    status: str = "pending",
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    params = {"status": status}
    sql = """
        SELECT id, farm_id, barangay, contributor_name, status,
               old_props_json, new_props_json, created_at, reviewed_at, reviewed_by
        FROM pending_farm_edits
        WHERE status = :status
    """
    if barangay:
        ensure_barangay(barangay)
        sql += " AND barangay = :barangay"
        params["barangay"] = barangay

    sql += " ORDER BY created_at DESC, id DESC"

    rows = db.execute(text(sql), params).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r[0],
            "farm_id": r[1],
            "barangay": r[2],
            "contributor_name": r[3],
            "status": r[4],
            "old_props": json.loads(r[5]),
            "new_props": json.loads(r[6]),
            "created_at": r[7],
            "reviewed_at": r[8],
            "reviewed_by": r[9],
        })
    return {"ok": True, "items": items}


@app.post("/api/pending-edits/{edit_id}/approve")
def approve_pending_edit(
    edit_id: int,
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    row = db.execute(text("""
        SELECT id, farm_id, barangay, status, new_props_json
        FROM pending_farm_edits
        WHERE id = :id
    """), {"id": edit_id}).fetchone()

    if not row:
        raise HTTPException(404, "Pending edit not found")

    if row[3] != "pending":
        raise HTTPException(400, "This request is already reviewed.")

    farm = db.query(Farm).filter(
        Farm.barangay == row[2],
        Farm.farm_id == row[1]
    ).first()

    if not farm:
        raise HTTPException(404, "Farm not found")

    new_props = json.loads(row[4])
    farm.props_json = json.dumps(new_props, ensure_ascii=False)

    db.execute(text("""
        UPDATE pending_farm_edits
        SET status = 'approved',
            reviewed_at = CURRENT_TIMESTAMP,
            reviewed_by = 'admin'
        WHERE id = :id
    """), {"id": edit_id})

    db.commit()
    return {"ok": True, "message": "Pending edit approved and applied."}


@app.post("/api/pending-edits/{edit_id}/reject")
def reject_pending_edit(
    edit_id: int,
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    row = db.execute(text("""
        SELECT id, status
        FROM pending_farm_edits
        WHERE id = :id
    """), {"id": edit_id}).fetchone()

    if not row:
        raise HTTPException(404, "Pending edit not found")

    if row[1] != "pending":
        raise HTTPException(400, "This request is already reviewed.")

    db.execute(text("""
        UPDATE pending_farm_edits
        SET status = 'rejected',
            reviewed_at = CURRENT_TIMESTAMP,
            reviewed_by = 'admin'
        WHERE id = :id
    """), {"id": edit_id})

    db.commit()
    return {"ok": True, "message": "Pending edit rejected."}

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Body, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
import json
from typing import Any, Dict, Optional

from db import get_db, engine
from models import Base, Farm, Boundary, Lulc

# Ensure tables exist
Base.metadata.create_all(bind=engine)

# Create contributors table WITHOUT editing models.py
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS contributors (
            name TEXT PRIMARY KEY,
            passkey TEXT NOT NULL
        )
    """))

app = FastAPI()

# Dev-friendly CORS (ok for local use)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# ADMIN API KEY PROTECTION
# =========================
API_KEY = "admin123"

def require_api_key(x_api_key: str = Header(default=None, alias="X-API-Key")):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# Keep same barangay keys your frontend already uses
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
        features.append({"type": "Feature", "properties": props, "geometry": geom})
    return {"type": "FeatureCollection", "features": features}


# =========================
# CONTRIBUTORS (ADMIN REGISTRATION)
# =========================
def contributor_ok(db: Session, name: str, key: str) -> bool:
    row = db.execute(
        text("SELECT passkey FROM contributors WHERE name = :n"),
        {"n": name}
    ).fetchone()
    if not row:
        return False
    return (row[0] == key)

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
        text("INSERT INTO contributors(name, passkey) VALUES(:n, :p) "
             "ON CONFLICT(name) DO UPDATE SET passkey = excluded.passkey"),
        {"n": name, "p": passkey}
    )
    db.commit()
    return {"ok": True, "message": f"Contributor '{name}' registered/updated."}


# =========================
# READ ONLY (VIEWER OK)
# =========================
@app.get("/api/farms")
def get_farms(barangay: str, db: Session = Depends(get_db)):
    ensure_barangay(barangay)
    rows = db.query(Farm).filter(Farm.barangay == barangay).all()
    return _fc_from_rows(rows)

@app.get("/api/boundary")
def get_boundary(barangay: str, db: Session = Depends(get_db)):
    ensure_barangay(barangay)
    rows = db.query(Boundary).filter(Boundary.barangay == barangay).all()
    return _fc_from_rows(rows)

@app.get("/api/lulc")
def get_lulc(barangay: str, db: Session = Depends(get_db)):
    ensure_barangay(barangay)
    rows = db.query(Lulc).filter(Lulc.barangay == barangay).all()
    return _fc_from_rows(rows)


# =========================
# WRITE (ADMIN ONLY)
# =========================
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
            id=farm_id,
            barangay=barangay,
            geom_geojson=json.dumps(geom, ensure_ascii=False),
            props_json=json.dumps(props, ensure_ascii=False),
        ))

    db.commit()
    return {"ok": True, "message": f"Farms uploaded for {barangay}."}


# =========================
# UPDATE FARM (ADMIN OR CONTRIBUTOR)
# =========================
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

    row = db.query(Farm).filter(Farm.barangay == barangay, Farm.id == farm_id).first()
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

    # Contributor restrictions:
    # - Can ONLY edit farms where stored farmer == contributor name
    # - Cannot edit geometry
    # - Cannot change farmer field
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

    props = stored_props

    for k, v in props_in.items():
        if k == "id":
            continue
        if is_contrib and k == "farmer":
            continue
        props[k] = v

    if is_contrib:
        props["farmer"] = stored_props.get("farmer", "")

    props["id"] = farm_id
    row.props_json = json.dumps(props, ensure_ascii=False)

    if geom is not None:
        if not is_admin:
            raise HTTPException(403, "Contributors cannot edit geometry.")
        row.geom_geojson = json.dumps(geom, ensure_ascii=False)

    db.commit()
    return {"ok": True}


@app.delete("/api/farms/{farm_id}")
def delete_farm(
    barangay: str,
    farm_id: int,
    db: Session = Depends(get_db),
    _auth: Any = Depends(require_api_key),
):
    ensure_barangay(barangay)
    row = db.query(Farm).filter(Farm.barangay == barangay, Farm.id == farm_id).first()
    if not row:
        raise HTTPException(404, "Farm not found")
    db.delete(row)
    db.commit()
    return {"ok": True}

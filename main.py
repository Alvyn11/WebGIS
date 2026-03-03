from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Body
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
import json
from typing import Any, Dict, Optional

from db import get_db, engine
from models import Base, Farm, Boundary, Lulc

# Ensure tables exist
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Dev-friendly CORS (ok for local use)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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


def empty_fc():
    return {"type": "FeatureCollection", "features": []}


def _read_upload_geojson(file: UploadFile) -> Dict[str, Any]:
    fname = (file.filename or "").lower()
    if not (fname.endswith(".geojson") or fname.endswith(".json")):
        raise HTTPException(400, "Please upload a GeoJSON file (.geojson or .json).")

    raw = file.file.read()
    if isinstance(raw, bytes):
        raw_bytes = raw
    else:
        raw_bytes = bytes(raw)

    try:
        obj = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Could not read JSON. Ensure it is valid UTF-8 GeoJSON.")

    validate_feature_collection(obj)
    if len(obj["features"]) < 1:
        raise HTTPException(400, "GeoJSON has no features.")
    return obj


def _fc_from_rows(rows):
    features = []
    for r in rows:
        geom = json.loads(r.geom_geojson)
        props = json.loads(r.props_json)
        features.append({"type": "Feature", "properties": props, "geometry": geom})
    return {"type": "FeatureCollection", "features": features}


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


@app.post("/api/upload-boundary")
async def upload_boundary(barangay: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    ensure_barangay(barangay)

    raw = await file.read()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Could not read JSON. Ensure it is valid UTF-8 GeoJSON.")

    validate_feature_collection(obj)
    if len(obj["features"]) < 1:
        raise HTTPException(400, "Boundary GeoJSON has no features.")

    # Replace all boundaries for this barangay
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
async def upload_lulc(barangay: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    ensure_barangay(barangay)

    raw = await file.read()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Could not read JSON. Ensure it is valid UTF-8 GeoJSON.")

    validate_feature_collection(obj)
    if len(obj["features"]) < 1:
        raise HTTPException(400, "LULC GeoJSON has no features.")

    # Replace all lulc for this barangay
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
async def upload_farms(barangay: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    ensure_barangay(barangay)

    raw = await file.read()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Could not read JSON. Ensure it is valid UTF-8 GeoJSON.")

    validate_feature_collection(obj)
    if len(obj["features"]) < 1:
        raise HTTPException(400, "Farm GeoJSON has no features.")

    # Replace farms for this barangay
    db.query(Farm).filter(Farm.barangay == barangay).delete()
    db.commit()

    # Generate unique IDs if missing
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

        # Remove NDVI related fields (if any)
        props.pop("ndvi_last", None)
        props.pop("ndvi_peak", None)
        props.pop("ndvi_drop", None)
        props.pop("status", None)

    # Insert new farms
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


@app.put("/api/farms/{farm_id}")
def update_farm(
    barangay: str,
    farm_id: int,
    payload: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
):
    ensure_barangay(barangay)
    row = db.query(Farm).filter(Farm.barangay == barangay, Farm.id == farm_id).first()
    if not row:
        raise HTTPException(404, "Farm not found")

    # Accept either:
    # 1) GeoJSON Feature {type, geometry, properties}
    # 2) Plain properties dict
    geom: Optional[Dict[str, Any]] = None
    props_in: Dict[str, Any] = {}

    if payload.get("type") == "Feature":
        geom = payload.get("geometry")
        props_in = payload.get("properties", {}) or {}
    else:
        props_in = payload

    # Update props (keep id stable)
    props = json.loads(row.props_json)
    for k, v in props_in.items():
        if k != "id":
            props[k] = v
    props["id"] = farm_id
    row.props_json = json.dumps(props, ensure_ascii=False)

    # Update geometry if provided
    if geom is not None:
        row.geom_geojson = json.dumps(geom, ensure_ascii=False)

    db.commit()
    return {"ok": True}


@app.delete("/api/farms/{farm_id}")
def delete_farm(barangay: str, farm_id: int, db: Session = Depends(get_db)):
    ensure_barangay(barangay)
    row = db.query(Farm).filter(Farm.barangay == barangay, Farm.id == farm_id).first()
    if not row:
        raise HTTPException(404, "Farm not found")
    db.delete(row)
    db.commit()
    return {"ok": True}

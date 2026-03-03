from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_URL = "sqlite:///farmgis.db"

engine = create_engine(
    DB_URL,
    connect_args={"check_same_thread": False},  # needed for SQLite + FastAPI
    future=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

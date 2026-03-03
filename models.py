from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Integer, Text


class Base(DeclarativeBase):
    pass


class Farm(Base):
    __tablename__ = "farms"

    # farm id (comes from properties.id or generated)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # barangay key (Poblacion, Minsalirac, San Isidro)
    barangay: Mapped[str] = mapped_column(Text, index=True)

    # geometry stored as JSON string (Polygon / MultiPolygon)
    geom_geojson: Mapped[str] = mapped_column(Text, nullable=False)

    # all properties stored as JSON string (crop, area, etc.)
    props_json: Mapped[str] = mapped_column(Text, nullable=False)


class Boundary(Base):
    __tablename__ = "boundaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    barangay: Mapped[str] = mapped_column(Text, index=True)

    geom_geojson: Mapped[str] = mapped_column(Text, nullable=False)
    props_json: Mapped[str] = mapped_column(Text, nullable=False)


class Lulc(Base):
    __tablename__ = "lulc"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    barangay: Mapped[str] = mapped_column(Text, index=True)

    geom_geojson: Mapped[str] = mapped_column(Text, nullable=False)
    props_json: Mapped[str] = mapped_column(Text, nullable=False)

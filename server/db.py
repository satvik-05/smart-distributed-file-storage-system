import os
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker

# Utilize Render's Production PostgreSQL if linked, otherwise natively fallback to an embedded SQLite local file
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///metadata.db")

# Render natively formats their database strings as `postgres://`, but SQLAlchemy requires `postgresql://`
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Connect natively with connection-pooling explicitly geared towards concurrent web traffic
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class FileRecord(Base):
    """Tracks the master record indicating which files exist anywhere on the P2P network."""
    __tablename__ = "files"
    file_name = Column(String, primary_key=True, index=True)
    uploader_id = Column(String, nullable=True)

class ChunkRecord(Base):
    """Tracks exactly how many fragment IDs belong to a specific FileRecord master."""
    __tablename__ = "chunks"
    chunk_id = Column(String, primary_key=True, index=True)
    file_name = Column(String, ForeignKey("files.file_name"), index=True)
    chunk_index = Column(Integer)

class ChunkLocation(Base):
    """Tracks the dynamic mappings indicating precisely WHICH client_id nodes hold WHICH chunk currently."""
    __tablename__ = "chunk_locations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chunk_id = Column(String, ForeignKey("chunks.chunk_id"), index=True)
    client_id = Column(String, index=True)

# Generate table architectures if they do not exist
Base.metadata.create_all(bind=engine)

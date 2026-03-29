import threading
import uuid
import random

from server.db import SessionLocal, FileRecord, ChunkRecord, ChunkLocation

class MetadataManager:
    """
    Manages the metadata for the distributed file system permanently.
    All indexing and chunk assigning logic leverages SQLAlchemy to completely 
    prevent memory loss during spontaneous Render spinning-down cycles!
    """
    def __init__(self):
        self.lock = threading.Lock()
        # We aim to keep 2 replicas of every chunk
        self.REPLICATION_FACTOR = 2

    def prepare_upload(self, file_name, num_chunks, available_clients, client_id=None):
        """
        Allocates chunk IDs and assigns them to available clients for uploading, 
        writing exactly to the PostgreSQL instances persistently.
        """
        with self.lock:
            if not available_clients:
                return False, "No clients are currently available to store the file."
                
            db = SessionLocal()
            try:
                existing = db.query(FileRecord).filter(FileRecord.file_name == file_name).first()
                if existing:
                    return False, "File already exists natively on the network tracking database."
                
                # Persist Master string record binding exact identity
                new_file = FileRecord(file_name=file_name, uploader_id=client_id)
                db.add(new_file)
                
                chunk_ids = []
                chunk_assignments = {}
                
                # Assign to distributed peers securely
                for idx in range(num_chunks):
                    chunk_id = str(uuid.uuid4())
                    chunk_ids.append(chunk_id)
                    
                    # Establish the relational indexing layout permanently
                    db.add(ChunkRecord(chunk_id=chunk_id, file_name=file_name, chunk_index=idx))
                    
                    num_replicas = min(self.REPLICATION_FACTOR, len(available_clients))
                    assigned_clients = random.sample(available_clients, num_replicas)
                    chunk_assignments[chunk_id] = assigned_clients
                    
                    # Bind physical chunk ID ownership to individual laptop nodes
                    for cid in assigned_clients:
                        db.add(ChunkLocation(chunk_id=chunk_id, client_id=cid))
                        
                db.commit()
                return True, {
                    "chunk_ids": chunk_ids,
                    "assignments": chunk_assignments
                }
            except Exception as e:
                db.rollback()
                return False, f"Permanent Database execution error: {str(e)}"
            finally:
                db.close()

    def get_file_locations(self, file_name):
        """
        Retrieves locations indicating which clients hold which chunks via DB mapping queries.
        """
        with self.lock:
            db = SessionLocal()
            try:
                file_record = db.query(FileRecord).filter(FileRecord.file_name == file_name).first()
                if not file_record:
                    return None
                    
                # Guarantee sequential reconstruction chunk reading logically ordered
                chunks_data = db.query(ChunkRecord).filter(ChunkRecord.file_name == file_name).order_by(ChunkRecord.chunk_index).all()
                chunk_ids = [c.chunk_id for c in chunks_data]
                
                locations = {}
                for cid in chunk_ids:
                    locs = db.query(ChunkLocation).filter(ChunkLocation.chunk_id == cid).all()
                    locations[cid] = [l.client_id for l in locs]
                    
                return {
                    "chunk_ids": chunk_ids,
                    "locations": locations
                }
            finally:
                db.close()

    def delete_file(self, file_name, requestor_id):
        """
        Validates the requestor natively owns the file, deletes all associated tracking mappings across the database, 
        and explicitly hands off chunk deletion routines back to the original user for native physical wiping.
        """
        with self.lock:
            db = SessionLocal()
            try:
                # Validate exact ownership securely via UUID match
                file_record = db.query(FileRecord).filter(FileRecord.file_name == file_name).first()
                if not file_record:
                    return False, "File not found on network registry.", None
                
                # If an uploader exists natively, enforce strict string matching
                if file_record.uploader_id and str(file_record.uploader_id) != str(requestor_id):
                    return False, "Unauthorized: You did not originally upload this file. Only the creator can revoke it.", None
                
                chunks_data = db.query(ChunkRecord).filter(ChunkRecord.file_name == file_name).all()
                chunk_ids = [c.chunk_id for c in chunks_data]
                
                locations_dict = {}
                for cid in chunk_ids:
                    locs = db.query(ChunkLocation).filter(ChunkLocation.chunk_id == cid).all()
                    locations_dict[cid] = [l.client_id for l in locs]
                
                locations = {
                    "chunk_ids": chunk_ids,
                    "locations": locations_dict
                }
                
                # Eradicate Database Tracking Context completely!
                for cid in chunk_ids:
                    db.query(ChunkLocation).filter(ChunkLocation.chunk_id == cid).delete(synchronize_session=False)
                
                db.query(ChunkRecord).filter(ChunkRecord.file_name == file_name).delete(synchronize_session=False)
                db.query(FileRecord).filter(FileRecord.file_name == file_name).delete(synchronize_session=False)
                
                db.commit()
                return True, "File metadata logically erased. Native chunks are now orphaned and returning locations.", locations
            except Exception as e:
                db.rollback()
                return False, f"Permanent Database deletion error: {str(e)}", None
            finally:
                db.close()

    def handle_dead_client(self, client_id):
        """
        Operates a swift SQL Delete query ensuring if clients drop completely offline, 
        they no longer pollute routing traffic requests.
        """
        with self.lock:
            db = SessionLocal()
            try:
                db.query(ChunkLocation).filter(ChunkLocation.client_id == client_id).delete()
                db.commit()
            except Exception:
                db.rollback()
            finally:
                db.close()

    def list_files(self):
        """
        Query selecting all master files distributed natively on network.
        """
        with self.lock:
            db = SessionLocal()
            try:
                files_query = db.query(FileRecord).all()
                return [f.file_name for f in files_query]
            finally:
                db.close()

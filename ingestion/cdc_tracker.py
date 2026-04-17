import os
import hashlib
from datetime import datetime
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class ProcessedDocument(Base):
    __tablename__ = 'processed_documents'
    file_path = Column(String, primary_key=True)
    file_hash = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow)


class CDCTracker:
    def __init__(self, db_path="sqlite:///cdc_state.db"):
        self.engine = create_engine(db_path)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def calculate_hash(self, file_path: str) -> str:
        """Calculates the SHA-256 hash of a file."""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()

    def needs_processing(self, file_path: str) -> bool:
        """Checks if a file is new or has been modified since last ingestion."""
        if not os.path.exists(file_path):
            return False

        current_hash = self.calculate_hash(file_path)
        session = self.Session()

        doc = session.query(ProcessedDocument).filter_by(file_path=file_path).first()
        session.close()

        if doc is None:
            return True
        if doc.file_hash != current_hash:
            return True

        return False

    def mark_as_processed(self, file_path: str):
        """Updates the database with the new file hash after successful ingestion."""
        current_hash = self.calculate_hash(file_path)
        session = self.Session()

        doc = session.query(ProcessedDocument).filter_by(file_path=file_path).first()
        if doc:
            doc.file_hash = current_hash
            doc.processed_at = datetime.utcnow()
        else:
            new_doc = ProcessedDocument(file_path=file_path, file_hash=current_hash)
            session.add(new_doc)

        session.commit()
        session.close()
import os
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

# Define the declarative base for our ORM models
Base = declarative_base()

load_dotenv()

class ProcessedDocument(Base):
    """ORM Model for tracking ingested files (Change Data Capture)."""
    __tablename__ = 'processed_documents'
    file_path = Column(String, primary_key=True)
    file_hash = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow)


class PostgresClient:
    def __init__(self):
        # Expects standard Postgres connection string in environment variables
        # Format: postgresql://username:password@host:port/database_name
        db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/investment_db")

        # pool_pre_ping tests the connection before using it (prevents timeouts in long pipelines)
        self.engine = create_engine(db_url, pool_pre_ping=True)

        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)

        # Create a configured "Session" class
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self):
        """Context manager to yield a database session and ensure it closes."""
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()
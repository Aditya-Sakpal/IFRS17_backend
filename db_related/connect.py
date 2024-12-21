from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from constants.misc import DB_URL
from models.user_management import Base

engine=create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

def get_db():
    """
        This function is used to get the database connection
        
        Returns:
            db: database connection object  
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
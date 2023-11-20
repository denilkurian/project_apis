from sqlalchemy import create_engine, Column, Integer, String,DateTime,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker, relationship






import urllib.parse

# Original password with special characters
password = "denil_kurian@123"

# Encode the password
encoded_password = urllib.parse.quote_plus(password)

# Construct the SQLAlchemy connection string
SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://root:{encoded_password}@localhost:3306/Testing"


engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()



class Connection(Base):

    __tablename__ = "connection"
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(255),nullable = True,index = True)
    host = Column(String(255),nullable = True,index = True)
    user = Column(String(255),nullable = True,index = True)
    password = Column(String(255),nullable = True,index = True)
    port = Column(Integer)
    database = Column(String(255),nullable = True,index = True)








class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    description = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())







class PipelineStatus(Base):
    __tablename__ = "pipeline_status"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    status = Column(String(250))
    created_at = Column(DateTime, server_default=func.now())

    pipeline = relationship("Pipeline", back_populates="statuses")

Pipeline.statuses = relationship("PipelineStatus", order_by=PipelineStatus.created_at, back_populates="pipeline")




def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


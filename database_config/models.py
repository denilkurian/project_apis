from sqlalchemy import create_engine, Column, Integer, String,DateTime,ForeignKey,DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Enum



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


########## all connection
class Connection(Base):

    __tablename__ = "connection"
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(255),nullable = True,index = True)
    host = Column(String(255),nullable = True,index = True)
    user = Column(String(255),nullable = True,index = True)
    password = Column(String(255),nullable = True,index = True)
    port = Column(Integer)
    database = Column(String(255),nullable = True,index = True)







############## pipelines
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









########## metadata
class IngestionMetadata(Base):
    __tablename__ = "metadata"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String(255))
    ingestion_datetime = Column(DateTime, server_default=func.now())
    executed_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(250))


class TransformationMetadata(Base):
    __tablename__ = "transformation"

    id = Column(Integer, primary_key=True, index=True)
    ingestion_id = Column(Integer, ForeignKey("metadata.id"))
    transformation_start_datetime = Column(DateTime, server_default=func.now())
    transformation_end_datetime = Column(DateTime)
    status = Column(Enum("Running","Completed","Failed"),nullable=False)
    error_message = Column(String(255))

    # relation = relationship("IngestionMetadata", back_populates="transformations")


class MetricsMetadata(Base):
    __tablename__ = "metricmetadata"

    metric_id = Column(Integer, primary_key=True, index=True)
    transformation_id = Column(Integer, ForeignKey("transformation.id"))
    metric_name = Column(String(250))
    metric_value = Column(DECIMAL(10,2),nullable=False)


class  PipelineMetadata(Base):
    __tablename__ = "pipelinemetadata"

    pipeline_id = Column(Integer, primary_key=True, index=True)
    pipeline_name = Column(String(250))
    pipeline_start_datetime = Column(DateTime, server_default=func.now())
    pipeline_end_datetime = Column(DateTime, server_default=func.now())
    status = Column(Enum("Running","Completed","Failed"),nullable=False)
    error_message = Column(String(255))


class PipelineExecution(Base):
    __tablename__ = "pipelineexecution"

    execution_id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelinemetadata.pipeline_id"))
    transformation_id = Column(Integer, ForeignKey("transformation.id"))
    execution_start_datetime = Column(DateTime, server_default=func.now())
    execution_end_datetime = Column(DateTime, server_default=func.now())
    status = Column(Enum("Running","Completed","Failed"),nullable=False)
    error_message = Column(String(255))


class JobMetadata(Base):
    __tablename__ = "jobmetadata"

    job_id = Column(Integer, primary_key=True, index=True)
    job_name = Column(String(255))
    job_type = Column(Enum("Ingestion","Transformation","Pipeline"),nullable=False)
    creation_datetime = Column(DateTime, server_default=func.now())


class JobExecutionStatus(Base):
    __tablename__ = "jobexecutionstatus"

    job_execution_id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer,ForeignKey("jobmetadata.job_id"))
    start_datetime = Column(DateTime, server_default=func.now())
    end_datetime = Column(DateTime, server_default=func.now())
    status = Column(Enum("Running","Completed","Failed"),nullable=False)
    error_message = Column(String(255))






def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()










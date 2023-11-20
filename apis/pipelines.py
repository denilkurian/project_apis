

# Please use the below in metadata ddl
# -- Create the pipeline table to store information about pipelines
# CREATE TABLE pipelines (
#     id SERIAL PRIMARY KEY,
#     name VARCHAR(200) NOT NULL,
#     description TEXT,
#     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
#     created_by varchar(200)
# );

# -- Create the pipeline_status table to track the status of pipelines over time
# CREATE TABLE pipeline_status (
#     id SERIAL PRIMARY KEY,
#     pipeline_id INT NOT NULL,
#     status VARCHAR(50) NOT NULL,
#     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
#     FOREIGN KEY (pipeline_id) REFERENCES pipelines (id)
# );



from fastapi import FastAPI,HTTPException,APIRouter
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from pydantic import BaseModel
from typing import List
from datetime import datetime
from database_config.models import SQLALCHEMY_DATABASE_URL as DATABASE_URL,Pipeline,PipelineStatus,Session




# Create a SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a FastAPI app
router = APIRouter()








# Pydantic models for data validation
class PipelineBase(BaseModel):
    name: str
    description: str

    class Config:
        orm_mode = True

class PipelineCreate(PipelineBase):
    pass

class Pipelines(PipelineBase):
    id: int
    created_at: datetime

    

class PipelineStatusBase(BaseModel):
    status: str


    class Config:
        orm_mode = True

class PipelineStatusCreate(PipelineStatusBase):
    pass

class PipelineStatuses(PipelineStatusBase):
    id: int
    pipeline_id: int
    created_at: datetime




# API endpoints to create, read, and list pipelines and their statuses
@router.post("/pipelines/",response_model=Pipelines)
def create_pipeline(pipeline: PipelineCreate):
    db_pipeline = Pipeline(**pipeline.dict())
    db = SessionLocal()
    db.add(db_pipeline)
    db.commit()
    db.refresh(db_pipeline)
    db.close()
    return db_pipeline





@router.get("/pipelines/{pipeline_id}",response_model=Pipelines)
def read_pipeline(pipeline_id: int):
    db = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    db.close()
    if pipeline is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline




@router.get("/pipelines/",response_model=List[Pipelines])
def list_pipelines(skip: int = 0, limit: int = 10):
    try:
        db: Session = SessionLocal()
        pipelines = db.query(Pipeline).offset(skip).limit(limit).all()
        db.close()
        return pipelines
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)}) 
    
        





@router.post("/pipelines/{pipeline_id}/status/",response_model=PipelineStatuses)
def create_pipeline_status(pipeline_id: int, status: PipelineStatusCreate):
    db = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if pipeline is None:
        db.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")
    db_status = PipelineStatus(pipeline_id=pipeline_id, **status.dict())
    db.add(db_status)
    db.commit()
    db.refresh(db_status)
    db.close()
    return db_status




@router.get("/pipelines/{pipeline_id}/status/",response_model=List[PipelineStatuses])
def list_pipeline_statuses(pipeline_id: int, skip: int = 0, limit: int = 20):
    db = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if pipeline is None:
        db.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")
    statuses = db.query(PipelineStatus).filter(PipelineStatus.pipeline_id == pipeline_id).offset(skip).limit(limit).all()
    db.close()
    return statuses















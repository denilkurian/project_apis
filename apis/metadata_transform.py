
from database_config.models import IngestionMetadata,get_db,TransformationMetadata
from fastapi import HTTPException,APIRouter,Depends
from sqlalchemy.orm import Session
from sqlalchemy import Enum
from datetime import datetime


router = APIRouter()


from pydantic import BaseModel

class IngestionData(BaseModel):
    source_name: str
    updated_by: str
    

class DataResponse(IngestionData):
    id: int


# Endpoint to ingest data into IngestionMetadata table
@router.post("/ingest",tags=["ingestion"])
def ingest_data(data:IngestionData,db:Session=Depends(get_db)):
    try:
        ingestion_data = IngestionMetadata(source_name=data.source_name,
                                        updated_by=data.updated_by)
        db.add(ingestion_data)
        db.commit()
        db.refresh(ingestion_data)
        db.close()

        response_data = DataResponse(
            source_name=ingestion_data.source_name,
            updated_by=ingestion_data.updated_by,
            id=ingestion_data.id
        )

        return {"message": "Data ingested successfully", "ingestion_id": response_data.dict()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





# Endpoint to get each ingestion id with details
@router.get("/ingest/{ingestion_id}/",tags=["ingestion"])
def get_ingest_data(ingestion_id:int,db:Session=Depends(get_db)):
    try:
        ingested_data = db.query(IngestionMetadata).filter(IngestionMetadata.id == ingestion_id).first()
        transformation_data = db.query(TransformationMetadata).filter(TransformationMetadata.ingestion_id == ingestion_id).all()
        db.close()
        return {"insertion_data":ingested_data,"transformation_data":transformation_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)})



# Endpoint to get all ingestion id with details
@router.get("/ingest/",tags=["ingestion"])
def get_all_ingest_data(skip: int = 0, limit: int = 10,db:Session=Depends(get_db)):
    try:
        ingested_data = db.query(IngestionMetadata).offset(skip).limit(limit).all()
        db.close()
        return {"insertion_data":ingested_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)})




# Endpoint to transformation metadata posting into Transformationmetadata table    
@router.post("/transformationmetadata/{ingestion_id}/",tags=["transformation"])
def transformation(ingestion_id:int,db:Session=Depends(get_db)):
    try:
         transform = db.query(IngestionMetadata).filter(IngestionMetadata.id == ingestion_id).first()
         if transform is None:
             db.close()
             return HTTPException(status_code=404, detail="no ingestion in this id")
         
         transformation_data = TransformationMetadata(ingestion_id=ingestion_id, status="Running")
         db.add(transformation_data)
         db.commit()
         db.refresh(transformation_data)
         db.close()
         return {"message": "Data ingested successfully", "transformation_id": transformation_data.id}
         
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))        
    

         


## update transformation metadata posting into Transformationmetadata table  

## for completion
@router.put("/transformationcomplete/{transformation_id}/", tags=["transformation"])
def update_transformation(transformation_id: int, db: Session = Depends(get_db)):
    try:
        transformation_data = db.query(TransformationMetadata).filter(TransformationMetadata.id == transformation_id).first()
        if transformation_data is None:
            db.close()
            raise HTTPException(status_code=404, detail=f"No transformation with id {transformation_id}")

        # Update the fields with the new data
        transformation_data.transformation_end_datetime = datetime.utcnow()
        transformation_data.status = "Completed"       
        db.commit()
        db.refresh(transformation_data)     
        db.close()

        return {"message": "Transformation data updated successfully", "transformation_id": transformation_data.id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
         


## for error              
@router.put("/transformationfailed/{transformation_id}/", tags=["transformation"])
def update_transformation(transformation_id: int, db: Session = Depends(get_db)):
    try:  
        transformation_data = db.query(TransformationMetadata).filter(TransformationMetadata.id == transformation_id).first()  
        if transformation_data is None:
            db.close()
            raise HTTPException(status_code=404, detail=f"No transformation with id {transformation_id}")

        # Update the fields with the new data
        transformation_data.transformation_end_datetime = datetime.utcnow()
        transformation_data.error_message = "Error job task"
        transformation_data.status = "Failed"       
        db.commit()
        db.refresh(transformation_data)     
        db.close()

        return {"message": "Transformation data updated successfully", "transformation_id": transformation_data.id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))         


        



                                       
                                       
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          














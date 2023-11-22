from fastapi import FastAPI
from database_config.models import Base, engine
import apis.metadata_transform as metadata_transform
import apis.apis as apis
import apis.pipelines as pipelines


app = FastAPI()


Base.metadata.create_all(bind=engine)

app.include_router(apis.router)
app.include_router(pipelines.router)
app.include_router(metadata_transform.router)





from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib
from sqlalchemy.orm import Session




import urllib.parse

# Original password with special characters
password = "denil_kurian@123"

# Encode the password
encoded_password = urllib.parse.quote_plus(password)

# Construct the SQLAlchemy connection string
SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://root:{encoded_password}@localhost/meta"


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


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()





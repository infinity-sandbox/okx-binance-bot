from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from app.services.database import Base  # Import Base from your database file

class User(Base):
    __tablename__ = 'users_test'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, index=True)

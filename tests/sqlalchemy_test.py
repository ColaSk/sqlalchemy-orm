from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine

config = {
    "url": 'sqlite:///test.db',
    "echo": True
}

engine = create_engine(**config)
session = sessionmaker(bind=engine)
ModelBase = declarative_base(bind=engine)


class User(ModelBase):

    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    name = Column(String(length=128))


ModelBase.metadata.create_all()

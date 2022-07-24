import sys
sys.path.append('../')

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine

from sqlalchemy_orm import MySQLAlchemy


config = {
    "url": 'sqlite:///test.db',
    "echo": True
}

db = MySQLAlchemy(config)


class User(db.Model):

    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    name = Column(String(length=128))


db.Model.metadata.create_all()

import os
import sys

from sqlalchemy import Column, Integer, String, DateTime, create_engine
from sqlalchemy.orm import relationship, scoped_session, sessionmaker, validates
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Index

engine = create_engine(os.environ["DB_URL"], convert_unicode=True, pool_recycle=3600)

sm = sessionmaker(autocommit=False,
                  autoflush=False,
                  bind=engine)

base_session = scoped_session(sm)

Base = declarative_base()
Base.query = base_session.query_property()


class Placement(Base):
    __tablename__ = 'placements'

    id = Column('id', Integer, primary_key=True)
    recieved_on = Column(DateTime)
    x = Column(Integer, nullable=False)
    y = Column(Integer, nullable=False)
    color = Column(Integer)
    author = Column(String)

Index("placement_recieved_on", Placement.recieved_on)
Index("placement_author", Placement.author)
Index("placement_color", Placement.color)

def init_db():
    engine.echo = True
    Base.metadata.create_all(bind=engine)

if "INITDB" in os.environ:
    init_db()
    sys.exit(0)

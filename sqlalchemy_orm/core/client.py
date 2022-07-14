import typing as t
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker, declarative_base
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.engine.url import make_url
from threading import Lock
from threading import get_ident as _ident_func

from .query import BaseQuery
from .model import Model
from .session import SignallingSession


class _EngineConnector(object):

    def __init__(self, sa, bind=None):
        self._sa = sa
        self._engine = None
        self._connected_for = None
        self._bind = bind
        self._lock = Lock()

    def get_uri(self): ...

    def get_engine(self): ...

    def get_options(self, sa_url, echo): ...


class SQLAlchemyClient(object):

    def __init__(
        self, 
        url: str, 
        query_class: BaseQuery = BaseQuery, 
        session_options: t.Optional[dict] = None,
        model_class:Model=Model,
        metadata=None,
        engine_options: t.Optional[dict]=None):
        
        self.url = url
        self.Query = query_class
        self.session_options = session_options
        self._engine_option = engine_options
        self._engine_lock = Lock()

        self.session = self.create_scoped_session(session_options)
        self.Model = self.make_declarative_base(model_class, metadata)


    def create_scoped_session(self, options: t.Optional[dict] = None):

        if options is None:
            options = {}

        scopefunc = options.pop('scopefunc', _ident_func)
        options.setdefault('query_cls', self.Query)
        return scoped_session(
            self.create_session(options), scopefunc=scopefunc
        )

    def make_declarative_base(self, model, metadata=None):        

        model = declarative_base(
            cls=model,
            name='Model',
            metadata=metadata
        )

        if metadata is not None and model.metadata is not metadata:
            model.metadata = metadata

        return model

    def create_session(self, options: dict):
        return sessionmaker(class_=SignallingSession, db=self, **options)

    @property
    def engine(self):
        return self.get_engine()

    def get_engine(self, bind=None):
        """Returns a specific engine."""

        with self._engine_lock:
            connector = self.connectors.get(bind)

            if connector is None:
                connector = self.make_connector(bind)
                self.connectors[bind] = connector

            return connector.get_engine()

    def make_connector(self, bind=None):
        """Creates the connector for a given state and bind."""
        return _EngineConnector(self, bind)
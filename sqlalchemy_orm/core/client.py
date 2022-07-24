import typing as t
import sqlalchemy
from sqlalchemy.orm import Session, scoped_session, sessionmaker, declarative_base
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.engine.url import make_url
from threading import Lock
from threading import get_ident as _ident_func

from .query import BaseQuery
from .model import Model
from .session import SignallingSession


# TODO：将被删除
class SQLAlchemyClient(object):

    def __init__(
        self,
        config: dict,
        query_class: BaseQuery = BaseQuery, 
        session_options: t.Optional[dict] = None,
        model_class: Model=Model,
        metadata=None,
        engine_options: t.Optional[dict]=None):

        self.connectors = {}
        
        self.config = config
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

    def make_declarative_base(self, model_class, metadata=None):        

        model = declarative_base(
            cls=model_class,
            name='Model'
        )

        if metadata is not None and model.metadata is not metadata:
            model.metadata = metadata
        
        if not getattr(model, 'query_class', None):
            model.query_class = self.Query

        return model

    def create_session(self, options: dict):
        return sessionmaker(class_=SignallingSession, db=self, **options)

    @property
    def engine(self):
        return self.get_engine()

    def get_engine(self, bind=None):

        with self._engine_lock:
            connector = self.connectors.get(bind)

            if connector is None:
                connector = self.make_connector(bind)
                self.connectors[bind] = connector

            return connector[bind]
    
    def get_tables_for_bind(self, bind=None):

        result = []
        for table in iter(self.Model.metadata.tables.values()):
            if table.info.get('bind_key') == bind:
                result.append(table)
        return result

    def get_binds(self):
        binds = [None] + list(self.config.get('SQLALCHEMY_BINDS') or ())
        retval = {}
        for bind in binds:
            engine = self.get_engine(bind)
            tables = self.get_tables_for_bind(bind)
            retval.update(dict((table, engine) for table in tables))
        return retval
    
    def get_uri(self, bind=None):
        if bind is None:
            return self.config['SQLALCHEMY_DATABASE_URI']
        binds = self.config.get('SQLALCHEMY_BINDS') or ()
        return binds[bind]

    def make_connector(self, bind=None):
        """Creates the connector for a given state and bind."""
        uri = self.get_uri(bind)
        echo = self.config.get('SQLALCHEMY_ECHO', None)
        sa_url = make_url(uri)
        sa_url, options = self.get_options(sa_url, echo)
        engine = self.create_engine(sa_url, options)

        return engine

    def get_options(self, sa_url, echo):
        options = {}

        options = self.apply_pool_defaults(options)
        sa_url, options = self.apply_driver_hacks(sa_url, options)

        if echo:
            options['echo'] = echo

        # Give the config options set by a developer explicitly priority
        # over decisions FSA makes.
        options.update(self.config['SQLALCHEMY_ENGINE_OPTIONS'])

        # Give options set in SQLAlchemy.__init__() ultimate priority
        options.update(self._sa._engine_options)

        return sa_url, options

    def apply_pool_defaults(self, options):

        def _setdefault(optionkey, configkey):
            value = self.config.get(configkey, None)
            if value is not None:
                options[optionkey] = value
        _setdefault('pool_size', 'SQLALCHEMY_POOL_SIZE')
        _setdefault('pool_timeout', 'SQLALCHEMY_POOL_TIMEOUT')
        _setdefault('pool_recycle', 'SQLALCHEMY_POOL_RECYCLE')
        _setdefault('max_overflow', 'SQLALCHEMY_MAX_OVERFLOW')
        return options


    def create_engine(self, sa_url, engine_opts):
        return sqlalchemy.create_engine(sa_url, **engine_opts)


# 迁移到此类

class MySQLAlchemy(object):

    """
    engine:
    session:
    model:
    """

    def __init__(
        self,
        config: t.Optional[dict] = None,
        session_options: t.Optional[dict] = None,
        engine_options: t.Optional[dict]=None):
        
        # 所有连接对象
        self._connectors = {}

        self._config = config if config else {}

        self._session_options = session_options
        self._engine_options = engine_options

        self._engine_lock = Lock()

        self.session = self.create_scoped_session(session_options)
        self.Model = self.make_declarative_base()

    
    def create_scoped_session(self, options: t.Optional[dict] = None):

        if options is None:
            options = {}

        scopefunc = options.pop('scopefunc', _ident_func)
        return scoped_session(
            self.create_session(options), scopefunc=scopefunc
        )
    
    @property
    def engine(self):
        return self.get_engine()
    
    def create_session(self, options: dict):
        return sessionmaker(class_=SignallingSession, db=self, **options)

    def get_binds(self):
        # 获取多数据库定义
        return

    def get_engine(self, bind=None):

        with self._engine_lock:

            connector = self._connectors.get(bind)

            if connector is None:
                connector = self.make_connector(bind)
                self._connectors[bind] = connector

            return connector

    
    def get_uri(self, bind=None):
        if bind is None:
            return self._config['url']
        binds = self._config.get('SQLALCHEMY_BINDS') or ()
        return binds[bind]

    
    def make_connector(self, bind=None):
        """Creates the connector for a given state and bind."""
        uri = self.get_uri(bind)
        options = {
            "echo": True
        } # TODO：根据url定义具体的options，针对不用数据库options存在支持与不支持的问题
        engine = self.create_engine(uri, options)

        return engine

    def create_engine(self, sa_url, engine_opts):
        return sqlalchemy.create_engine(sa_url, **engine_opts)

    def make_declarative_base(self): 

        model = declarative_base(
            bind=self.engine,
            name='Model'
        )

        return model


    
import typing as t
import warnings
import sqlalchemy
from sqlalchemy.orm import class_mapper, scoped_session, sessionmaker, declarative_base
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.engine.url import make_url
from threading import Lock
from threading import get_ident as _ident_func

from .query import BaseQuery
from .model import Model
from .session import SignallingSession

class _QueryProperty(object):
    def __init__(self, sa):
        self.sa = sa

    def __get__(self, obj, type):
        try:
            mapper = class_mapper(type)
            if mapper:
                return type.query_class(mapper, session=self.sa.session())
        except UnmappedClassError:
            return None


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
        engine_options: t.Optional[dict]=None,
        query_class: BaseQuery = BaseQuery,
        model_class: Model=Model,
        metadata=None):
        
        # 所有连接对象
        self._connectors = {}

        self._config = config or {}
        self._session_options = session_options
        self._engine_options = engine_options or {}
        self.Query = query_class
        self.model_class = model_class
        self.metadata = metadata

        self._engine_lock = Lock()

        self.check_config()

        self.session = self.create_scoped_session(session_options)
        self.Model = self.make_declarative_base(model_class, metadata)
        
        # TODO: include_sqlalchemy
    
    def check_config(self):
        """check config"""
        if (
            'SQLALCHEMY_DATABASE_URI' not in self._config and
            'SQLALCHEMY_BINDS' not in self._config
        ):
            warnings.warn(
                'Neither SQLALCHEMY_DATABASE_URI nor SQLALCHEMY_BINDS is set. '
                'Defaulting SQLALCHEMY_DATABASE_URI to "sqlite:///:memory:".'
            )

        self._config.setdefault('SQLALCHEMY_DATABASE_URI', 'sqlite:///:memory:')
        self._config.setdefault('SQLALCHEMY_BINDS', None)
        self._config.setdefault('SQLALCHEMY_NATIVE_UNICODE', None)
        self._config.setdefault('SQLALCHEMY_ECHO', False)
        self._config.setdefault('SQLALCHEMY_RECORD_QUERIES', None)
        self._config.setdefault('SQLALCHEMY_POOL_SIZE', None)
        self._config.setdefault('SQLALCHEMY_POOL_TIMEOUT', None)
        self._config.setdefault('SQLALCHEMY_POOL_RECYCLE', None)
        self._config.setdefault('SQLALCHEMY_MAX_OVERFLOW', None)
        self._config.setdefault('SQLALCHEMY_COMMIT_ON_TEARDOWN', False)
        self._config.setdefault('SQLALCHEMY_TRACK_MODIFICATIONS', None)
        self._config.setdefault('SQLALCHEMY_ENGINE_OPTIONS', {})

    def create_scoped_session(self, options: t.Optional[dict] = None):

        if options is None:
            options = {}

        scopefunc = options.pop('scopefunc', _ident_func)
        options.setdefault('query_cls', self.Query)
        return scoped_session(
            self.create_session(options), scopefunc=scopefunc
        )
    
    @property
    def engine(self):
        return self.get_engine()
    
    def create_session(self, options: dict):
        return sessionmaker(class_=SignallingSession, db=self, **options)

    def get_binds(self):
        """Returns a dictionary with a table->engine mapping.
        """
        binds = [None] + list(self._config.get('SQLALCHEMY_BINDS') or ())
        retval = {}
        for bind in binds:
            engine = self.get_engine(bind)
            tables = self.get_tables_for_bind(bind)
            retval.update(dict((table, engine) for table in tables))
        return retval
    
    def get_tables_for_bind(self, bind=None):

        result = []
        for table in iter(self.Model.metadata.tables.values()):
            if table.info.get('bind_key') == bind:
                result.append(table)
        return result

    def get_engine(self, bind: t.Optional[str] = None):
        """获取engine"""
        with self._engine_lock:
            connector = self._connectors.get(bind)

            if connector is None:
                connector = self.make_connector(bind)
                self._connectors[bind] = connector

            return connector
 
    def get_uri(self, bind=None):
        """获取uri"""
        if bind is None:
            return self._config['SQLALCHEMY_DATABASE_URI']
        binds = self._config.get('SQLALCHEMY_BINDS') or ()
        return binds[bind]

    def make_connector(self, bind=None):
        """Creates the connector for a given state and bind.
        1. 获取指定bind的uri
        2. 生成url
        3. 获取options
        4. 生成 engine
        """

        uri = self.get_uri(bind)
        echo = self._config.get('SQLALCHEMY_ECHO', None)
        url = make_url(uri)
        url, options = self.get_options(url, echo)
        engine = self.create_engine(url, options)

        return engine
 
    def get_options(self, sa_url, echo):
        options = {}

        options = self.apply_pool_defaults(options)
        sa_url, options = self.apply_driver_hacks(sa_url, options)

        if echo:
            options['echo'] = echo

        # Give the config options set by a developer explicitly priority
        # over decisions FSA makes.
        options.update(self._config['SQLALCHEMY_ENGINE_OPTIONS'])

        # Give options set in SQLAlchemy.__init__() ultimate priority
        options.update(self._engine_options)

        return sa_url, options

    def apply_pool_defaults(self, options: dict):

        def _setdefault(optionkey, configkey):
            """获取配置项中的配置转化为制定函数入参"""
            value = self._config.get(configkey, None)
            if value is not None:
                options[optionkey] = value

        _setdefault('pool_size', 'SQLALCHEMY_POOL_SIZE')
        _setdefault('pool_timeout', 'SQLALCHEMY_POOL_TIMEOUT')
        _setdefault('pool_recycle', 'SQLALCHEMY_POOL_RECYCLE')
        _setdefault('max_overflow', 'SQLALCHEMY_MAX_OVERFLOW')

        return options

    def apply_driver_hacks(self, url, options: dict):
        """This method is called before engine creation and used to inject
        driver specific hacks into the options.  The `options` parameter is
        a dictionary of keyword arguments that will then be used to call
        the :func:`sqlalchemy.create_engine` function.
        TODO: 完善
        """
        return url, options

    def create_engine(self, sa_url, engine_opts):
        return sqlalchemy.create_engine(sa_url, **engine_opts)

    def make_declarative_base(self, model_class, metadata=None): 

        model = declarative_base(
            cls=model_class,
            name='Model'
        )

        if metadata is not None and model.metadata is not metadata:
            model.metadata = metadata
        
        if not getattr(model, 'query_class', None):
            model.query_class = self.Query

        model.query = _QueryProperty(self)

        return model

    def _execute_for_all_tables(self, bind, operation, skip_tables=False):
       
        if bind == '__all__':
            binds = [None] + list(self._config.get('SQLALCHEMY_BINDS') or ())
        elif isinstance(bind, str) or bind is None:
            binds = [bind]
        else:
            binds = bind

        for bind in binds:
            extra = {}
            if not skip_tables:
                tables = self.get_tables_for_bind(bind)
                extra['tables'] = tables
            op = getattr(self.Model.metadata, operation)
            op(bind=self.get_engine(bind), **extra)

    def create_all(self, bind='__all__'):
        """Creates all tables.

        .. versionchanged:: 0.12
           Parameters were added
        """
        self._execute_for_all_tables(bind, 'create_all')

    def drop_all(self, bind='__all__'):
        """Drops all tables.

        .. versionchanged:: 0.12
           Parameters were added
        """
        self._execute_for_all_tables(bind, 'drop_all')
    
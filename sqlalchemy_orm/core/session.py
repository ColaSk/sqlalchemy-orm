from sqlalchemy.orm import Session as SessionBase

class SignallingSession(SessionBase):

    def __init__(self, db, autocommit=False, autoflush=True, **options):
        #: The application that this session belongs to.
        self.db = db
        bind = options.pop('bind', None) or db.engine
        binds = options.pop('binds', db.get_binds())

        SessionBase.__init__(
            self, autocommit=autocommit, autoflush=autoflush,
            bind=bind, binds=binds, **options
        )

    def get_bind(self, mapper=None, clause=None):
        """Return the engine or connection for a given model or
        table, using the ``__bind_key__`` if it is set.
        """
        # mapper is None if someone tries to just get a connection
        if mapper is not None:
            try:
                # SA >= 1.3
                persist_selectable = mapper.persist_selectable
            except AttributeError:
                # SA < 1.3
                persist_selectable = mapper.mapped_table

            info = getattr(persist_selectable, 'info', {})
            bind_key = info.get('bind_key')
            if bind_key is not None:
                return self.db.get_engine(bind=bind_key)
        return SessionBase.get_bind(self, mapper, clause)

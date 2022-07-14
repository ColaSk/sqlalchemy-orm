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

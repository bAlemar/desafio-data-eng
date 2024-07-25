import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from infra.db.settings.database_configs import database_infos
# Definindo database_infos antes de instanciar __DBConnectionHandler

class __DBConnectionHandler:

    def __init__(self):
        print('database_infos',database_infos)
        self.__connection_string = database_infos['DB_URL']

        self.engine = None
        self.session = None
        self.session_factory = None

    def connect_to_db(self):
        self.engine = create_engine(self.__connection_string)

    def get_engine(self):
        return self.engine

    def __enter__(self):
        self.connect_to_db()
        self.session_factory = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.session = self.session_factory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.engine.dispose()

# Instanciando db_connection_handler após definir database_infos
db_connection_handler = __DBConnectionHandler()

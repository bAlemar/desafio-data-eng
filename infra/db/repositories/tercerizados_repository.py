from sqlalchemy.orm.exc import NoResultFound
from infra.db.settings.connection import db_connection_handler
from typing import Dict

class TercerizadosRepository():
    @classmethod
    def load_dataframe_to_postgres(cls, df,table_name):
        with db_connection_handler as database:
            try:
                df.to_sql(table_name,database.engine,if_exists='replace',index=False)
                return 
            except NoResultFound:
                return None
            except Exception as exception:
                database.session.rollback()
                raise exception

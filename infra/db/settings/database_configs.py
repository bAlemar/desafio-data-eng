import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

database_infos = {
    "DB_URL": os.getenv("DB_URL"),
}

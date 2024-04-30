from pathlib import Path
import os
import platform
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from credentials import *

SYS = platform.system()
SYS_DEL = "/" if SYS.lower() in ["linux", "darwin"] else "\\"

APP_PATH = Path(os.path.dirname(os.path.abspath(__file__))) / "app"

CORE_DATABASE_PARAMS = {
    "dialect": "mysql",
    "driver": "aiomysql",
    "username": DB_USERNAME,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": "testing"
    }

# jwt token hash base, replace with whatever
ACCESS_TOKEN_SECRET_KEY = "LAzO5dTr!rr7OihL6itwh@4r7zDyHRaUdo3VGTWwWF8uPOSdBqNX?COlS3Tl&FeT"
ACCESS_TOKEN_ALGORITHM = "HS256"

# every token will expire in 60 minutes by default
ACCESS_TOKEN_EXPIRE_MINUTES = 60

OAUTH2_SCHEME = OAuth2PasswordBearer(tokenUrl="token")
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")


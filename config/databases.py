import os
from dotenv import load_dotenv

load_dotenv()

database_dw = {
    "server" : os.getenv('SERVER_MS'),
    "database" : os.getenv('DATABASE_MS'),
    "driver" : os.getenv('DRIVER_MS'),
    "user" : os.getenv('USERNAME_MS'),
    "password" : os.getenv('PASSWORD_MS')
}

database_aws = {
    "endpoint" : os.getenv('ENDPOINT_AWS'),
    "port" : os.getenv('PORT_AWSS'),
    "user" : os.getenv('USER_AWS'),
    "passwd" : os.getenv('PASSWD_AWS'),
    "region" : os.getenv('REGION_AWS'),
    "dbname" : os.getenv('DBNAME_AWS')
}

database_tasy = {
    "server" : os.getenv('SERVER_TASY'),
    "user" : os.getenv('USER_TASY'),
    "password" : os.getenv('PASSWORD_TASY')
}


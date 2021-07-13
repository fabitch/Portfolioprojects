from sqlalchemy import create_engine

USER = "root"
PW = "superSecretPassword"
PORT = 3306

engine = create_engine(f"mariadb+pymysql://{USER}:{PW}@0.0.0.0:{PORT}")

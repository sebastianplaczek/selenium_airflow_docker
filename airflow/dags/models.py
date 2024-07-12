from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    Sequence,
    inspect,
    DateTime,
    MetaData,
)
from sqlalchemy import inspect, create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()


class Offers(Base):
    __tablename__ = "offers"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    link = Column(String(255))
    type = Column(String(20))
    title = Column(String(255))
    create_date = Column(DateTime, default=datetime.now())
    seller = Column(String(255))
    seller_type = Column(String(255))
    page = Column(Integer)
    position = Column(Integer)
    bumped = Column(Boolean)
    offer_loc_id = Column(Integer)
    n_scrap = Column(Integer)
    address = Column(String(255))
    price = Column(Float)
    price_per_m = Column(Float)
    rooms = Column(Integer)
    floor = Column(Integer)
    size = Column(Float)
    additional_params = Column(String(1000))


class OffersLoc(Base):
    __tablename__ = "offers_loc"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    type = Column(String(20))
    address = Column(String(255))
    link = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    city = Column(String(50))
    municipality = Column(String(100))
    county = Column(String(100))
    vivodeship = Column(String(50))
    postcode = Column(String(10))
    create_date = Column(DateTime, default=datetime.now())
    filled = Column(Integer, default=0)
    additional_params = Column(String(1000))


class NominatimApi(Base):
    __tablename__ = "NominatimApi"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    link = Column(String(500))
    status_code = Column(Integer)
    create_date = Column(DateTime, default=datetime.now())
    empty = Column(Boolean)
    offer_id = Column(Integer)


class OtodomWebsite(Base):
    __tablename__ = "OtodomWebsite"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    type = Column(String(20))
    link = Column(String(255))
    active = Column(Boolean)
    num_pages = Column(Integer)
    create_date = Column(DateTime, default=datetime.now())


class ScrapInfo(Base):
    __tablename__ = "scrapinfo"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    create_date = Column(DateTime, default=datetime.now())
    active = Column(Boolean, default=True)


class CeleryTasks(Base):
    __tablename__ = "celery_tasks"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    task_id = Column(Integer)
    type = Column(String(20))
    create_date = Column(DateTime, default=datetime.now())
    status = Column(String(20))
    done = Column(Boolean, default=0)
    time_start = Column(DateTime)
    time_end = Column(DateTime)
    runtime = Column(Float)
    pages = Column(String(20))
    start_page = Column(String(20))
    threads = Column(Integer)


class Runtime(Base):
    __tablename__ = "runtime"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    n_offers = Column(Integer)
    threads = Column(Integer)
    time_s = Column(Float)
    time_per_offer = Column(Float)
    type = Column(String(20))
    page = Column(Integer)
    n_scrap = Column(Integer)
    create_date = Column(DateTime, default=datetime.now())


class ErrorLogs(Base):
    __tablename__ = "error_logs"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    type = Column(String(20))
    value_type = Column(String(20))
    value = Column(String(20))
    exception = Column(String(1000))


db_url = "mysql+pymysql://normal:qwerty123@172.22.0.2:3306/scrapper_db"
# db_url = "mysql+pymysql://normal:qwerty123@127.0.0.1:3307/scrapper_db"
engine = create_engine(
    db_url,
    pool_size=10,  # Domyślnie 5
    max_overflow=20,  # Domyślnie 10
    pool_timeout=30,  # Domyślnie 30 sekund
    pool_recycle=1800,  # Recykluj połączenia co 30 minut
)


# Funkcja do sprawdzania i tworzenia tabel, jeśli nie istnieją


def create_tables_and_columns_if_not_exists(engine, base):
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    metadata = MetaData(bind=engine)
    tables_to_create = []

    # Loop through all table classes
    for table_class in base.__subclasses__():
        test = base.__subclasses__()
        if hasattr(table_class, "__tablename__"):
            table_name = table_class.__tablename__
            if table_name not in existing_tables:
                tables_to_create.append(table_class.__table__)
            else:
                # Check for missing columns
                table = table_class.__table__
                existing_columns = {
                    col["name"] for col in inspector.get_columns(table_name)
                }
                for column in table.columns:
                    if column.name not in existing_columns:
                        column_type = column.type.compile(engine.dialect)
                        with engine.connect() as conn:
                            conn.execute(
                                f"ALTER TABLE {table_name} ADD COLUMN {column.name} {column_type}"
                            )

    # Create any new tables
    if tables_to_create:
        base.metadata.create_all(engine, tables=tables_to_create)


# Utwórz tabele tylko jeśli nie istnieją
create_tables_and_columns_if_not_exists(engine, Base)

# Utworzenie sesji
Session = sessionmaker(bind=engine)

print("Done")

from sqlalchemy import (
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
from sqlalchemy import inspect, MetaData
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from main_scraper import DatabaseManager


DATABASE = "gcp"
Base = declarative_base()


class OtodomOffers(Base):
    __tablename__ = "OtodomOffers"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    link = Column(String(255))
    type = Column(String(20))
    title = Column(String(255))
    create_date = Column(DateTime, default=datetime.now())
    data_date = Column(DateTime, default=datetime.now())
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


class ErrorLogs(Base):
    __tablename__ = "error_logs"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    type = Column(String(20))
    value_type = Column(String(20))
    value = Column(String(20))
    exception = Column(String(1000))


class PracujJobOffers(Base):
    __tablename__ = "PracujJobOffers"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    type = Column(String(255))
    link = Column(String(255))
    title = Column(String(255))
    salary = Column(String(255))
    company = Column(String(255))
    location = Column(String(255))
    tags = Column(String(255))
    additional_info = Column(String(1000))
    date_pub = Column(String(50))
    create_date = Column(DateTime)
    data_date = Column(DateTime, default=datetime.now())


class ModelsToDb:

    def __init__(self):
        self.base = Base
        dbmanager = DatabaseManager(database=DATABASE)
        self.engine = dbmanager.engine
        self.create_tables_and_columns_if_not_exists()

    def create_tables_and_columns_if_not_exists(self):
        inspector = inspect(self.engine)
        existing_tables = inspector.get_table_names()
        metadata = MetaData(bind=self.engine)
        tables_to_create = []
        test = self.base.__subclasses__()
        # Loop through all table classes
        for table_class in self.base.__subclasses__():
            test = self.base.__subclasses__()
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
                            column_type = column.type.compile(self.engine.dialect)
                            with self.engine.connect() as conn:
                                conn.execute(
                                    f"ALTER TABLE {table_name} ADD COLUMN {column.name} {column_type}"
                                )
        if tables_to_create:
            self.base.metadata.create_all(self.engine, tables=tables_to_create)


if __name__ == "__main__":
    ModelsToDb()

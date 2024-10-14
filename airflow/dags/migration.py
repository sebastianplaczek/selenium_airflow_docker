from sqlalchemy import MetaData

from main_scraper import DatabaseManager,ConfigReader


class DatabaseMigrator():

    def __init__(self,source_db,target_db):
        self.db_man1 = DatabaseManager(database=source_db)
        self.db_man2 = DatabaseManager(database=target_db)

        self.db1_engine = self.db_man1.engine
        self.db2_engine = self.db_man2.engine

        self.run()

    def create_empty_tables(self):

        self.metadata = MetaData()
        self.metadata.reflect(bind=self.db1_engine)
        self.metadata.create_all(bind=self.db2_engine)

    def migrate_table_in_batches(self,table, batch_size=10000):
        offset = 0
        print(table)
        batch_n = 1
        while True:
            print(f"Batch nr {batch_n}")
            query = table.select().offset(offset).limit(batch_size)
            batch = self.db1_engine.execute(query).fetchall()

            if not batch:
                break

            self.db2_engine.execute(table.insert(), [dict(row) for row in batch])
            offset += batch_size
            batch_n += 1
    
    def run(self):
        self.create_empty_tables()

        for table in self.metadata.sorted_tables:
            self.migrate_table_in_batches(table)

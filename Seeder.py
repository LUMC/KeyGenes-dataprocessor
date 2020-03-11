import os
from Database import db
from dotenv import load_dotenv
import settings

class Seeder(db):

    def __init__(self,host, username, password, database, gene_ref_file, gene_count_file):
        super().__init__(host, username, password)
        self.database = database
        self.gene_ref_file= gene_ref_file
        self.gene_count_file= gene_count_file

    def insert_gene_ref(self):
        self.connect_to_db(self.database)
        with open(self.gene_ref_file) as f:
            for line in f:
                line = line.replace('\n', '').replace('\r', '')
                items = line.split(';')
                self.connection.execute("INSERT INTO gene (ensg, symbol, description) VALUES ('{ensg}', '{symbol}', '{desc}')".format(
                    ensg=items[0], symbol=items[2], desc=items[1]
                ))
        self.connection.close()


    def prepare_data(self, stage_file, tissue_file):
        stages = {}
        tissues = {}
        with open(stage_file) as f:
            for line in f:
                line = line.replace('\n', '').replace('\r', '')
                line = line.split(',')
                stages[line[4]] = line[0]
        f.close()
        with open(tissue_file) as f:
            for line in f:
                line = line.replace('\n', '').replace('\r', '')
                line = line.split(',')
                tissues[line[4]] = line[0]
        f.close()
        return stages, tissues

    def insert_count(self, stage_file, tissue_file):
        stages, tissues = self.prepare_data(stage_file,tissue_file )
        self.connect_to_db(self.database)
        self.connection.execute('DELETE FROM transcript;')
        with open(self.gene_count_file) as f:
            for line in f:
                line = line.replace('\n', '').replace('\r', '')
                items = line.split(';')
                result = self.connection.execute("SELECT id FROM gene WHERE ensg = '{gname}'".format(gname=items[0]))
                self.connection.execute("INSERT INTO transcript (gene, stage, tissue, count) VALUES ('{gene}', '{stage}', '{tissue}', '{count}')".format(
                    gene=[item[0] for item in result][0], stage=stages[items[2]], tissue=tissues[items[1]],
                    count=items[3]
                ))
        self.connection.close()

if __name__ == '__main__':
    HOST = os.getenv("HOST")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    DATABASE = os.getenv("DATABASE")
    REF_FILE = os.getenv("REF_FILE")
    COUNT_FILE = os.getenv("COUNT_FILE")
    print(HOST, USERNAME, PASSWORD, DATABASE, REF_FILE, COUNT_FILE)
    seeder = Seeder(
        host=HOST,
        username=USERNAME,
        password=PASSWORD,
        database=DATABASE,
        gene_ref_file=REF_FILE,
        gene_count_file=COUNT_FILE,
    )
    seeder.insert_gene_ref()
    seeder.insert_count(stage_file='datasets/stage.csv', tissue_file='datasets/tissue.csv')
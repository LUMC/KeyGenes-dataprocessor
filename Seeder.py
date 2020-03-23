import os, time, sys
from Database import db
from Merger import GeneAliasRetriever
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import settings


class Seeder(db):

    def __init__(self, dialect, driver, host, username, password, database, gene_ref_file, gene_count_file):
        super().__init__(dialect, driver, host, username, password)
        self.database = database
        self.gene_ref_file= gene_ref_file
        self.gene_count_file= gene_count_file

    def insert_gene_ref(self):
        print('Preparing....')
        num_lines = sum(1 for line in open(self.gene_ref_file))
        self.connect_to_db(self.database)
        self.connection.execute('DELETE FROM gene;')
        row = 1
        with open(self.gene_ref_file) as f:
            for line in f:
                line = line.replace('\n', '').replace('\r', '')
                items = line.split(';')
                try:
                    if len(items) < 3:
                        self.connection.execute("INSERT INTO gene (ensg) VALUES ('{ensg}')".format(
                            ensg=items[0]
                        ))
                    else:
                        self.connection.execute("INSERT INTO gene (ensg, symbol, description) VALUES (%s, %s, %s)",
                                                [items[0], items[2], items[1]]
                        )
                    print("Inserted gene: {gene} | {line}/{lines}".format(
                        gene=items[0],
                        line=row,
                        lines=num_lines
                    ))
                    row += 1
                except:
                    print('!!! Error adding: {}'.format(items[0]))
                    time.sleep(5)
        print('--- All genes inserted! ---')
        self.connection.close()

    def write_log(self, content):
        if os.path.exists('log.txt'):
            append_write = 'a'  # append if already exists
        else:
            append_write = 'w'  # make a new file if not

        highscore = open('.log', append_write)
        highscore.write(content+"\n")
        highscore.close()

    def update_gene(self, gene_id, db_ensg, old_ensg, new_ensg=None):
        if new_ensg:
            result = GeneAliasRetriever.get_alias(old_ensg)
            if len(result) < 2:
                result = GeneAliasRetriever.get_alias(new_ensg)
        else:
            result = GeneAliasRetriever.get_alias(old_ensg)
        if len(result) > 2:
            try:
                self.connection.execute("UPDATE gene SET ensg =%s, description =%s, symbol=%s, old_ensg=%s WHERE id =%s",
                                    [result[0], result[1], result[2], db_ensg, gene_id]
                                    )
            except SQLAlchemyError as e:
                error = str(e.__dict__['orig'])
                if "Duplicate entry" in error:
                    self.write_log(error)
                    self.connection.execute("DELETE FROM gene WHERE id='{id}'".format(id=gene_id))
                print(error)

    def update_genes(self, gene_items):
        self.connect_to_db(self.database)
        for item in gene_items:
            result = self.connection.execute("SELECT id, ensg FROM gene WHERE ensg='{old_ensg}'".format(
                            old_ensg=item[0].split(".")[0]
                        ))
            gene = [item for item in result]
            if gene:
                gene = gene[0]
                gene_id = gene[0]
                ensg = gene[1]
                if len(item) > 1:
                    self.update_gene(gene_id, ensg, item[0], item[1])
                else:
                    self.update_gene(gene_id, ensg, item[0])
        self.connection.close()

    def correct_genes(self, reference_file):
        gene_items = []
        with open(reference_file) as f:
            for line in f:
                line = line.replace("\n", "")
                items = line.split(" ")
                gene_items.append(items)
        self.update_genes(gene_items)



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
        row = 1
        print('Preparing....')
        num_lines = sum(1 for line in open(self.gene_count_file))
        stages, tissues = self.prepare_data(stage_file,tissue_file )
        self.connect_to_db(self.database)
        self.connection.execute('DELETE FROM transcript;')
        with open(self.gene_count_file) as f:
            for line in f:
                line = line.replace('\n', '').replace('\r', '')
                items = line.split(';')
                result = self.connection.execute("SELECT id FROM gene WHERE ensg = '{gname}'".format(gname=items[0]))
                try:
                    self.connection.execute("INSERT INTO transcript (gene, stage, tissue, count) VALUES ('{gene}', '{stage}', '{tissue}', '{count}')".format(
                        gene=[item[0] for item in result][0], stage=stages[items[2]], tissue=tissues[items[1]],
                        count=items[3]
                    ))
                    print("Inserted gene count: {gene} | {line}/{lines}".format(
                        gene=items[0],
                        line=row,
                        lines=num_lines
                    ))
                    row += 1
                except:
                    print('!!! Error adding count: {}'.format(items[0]))
                    time.sleep(5)
        print('--- All counts inserted! ---')
        self.connection.close()


if __name__ == '__main__':
    DB_DIALECT = os.getenv("DB_DIALECT")
    DB_DRIVER = os.getenv("DB_DRIVER")
    HOST = os.getenv("HOST")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    DATABASE = os.getenv("DATABASE")
    REF_FILE = os.getenv("REF_FILE")
    COUNT_FILE = os.getenv("COUNT_FILE")
    seeder = Seeder(
        dialect=DB_DIALECT,
        driver=DB_DRIVER,
        host=HOST,
        username=USERNAME,
        password=PASSWORD,
        database=DATABASE,
        gene_ref_file=REF_FILE,
        gene_count_file=COUNT_FILE,
    )
    # seeder.insert_gene_ref()
    # seeder.insert_count(stage_file='datasets/stage.csv', tissue_file='datasets/tissue.csv')
    seeder.correct_genes("updated_genes.txt")
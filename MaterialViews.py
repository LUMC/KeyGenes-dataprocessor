import sqlalchemy


class MaterialViews():

    def __init__(self, db):
        self.db = db
        self.run_mv_transcript()

    def run_mv_transcript(self):
        with open('mviews/transcript_mv.sql') as file:
            query = file.read()
        self.prepare(query)
        result = self.db.execute("SELECT id FROM tissue")
        tissues = [item for item in result]
        for tissue in tissues:
            tissue_id = tissue[0]
            query = "SELECT  gene, tissue, avg(count) from transcript "
            query += "WHERE tissue = {tissue} ".format(tissue=tissue_id)
            query += "GROUP BY gene "
            query += "ORDER BY avg(count) DESC "
            query += "LIMIT 100 "
            result = self.db.execute(query)
            for item in result:
                query = "INSERT INTO transcript_mv (gene, tissue, count_avg) "
                query += "VALUES ({x[0]}, {x[1]}, {x[2]})".format(x=list(item))
                self.db.execute(query)
        self.db.close()

    def prepare(self, file):
        sql_command = ''
        for line in file:
            if not line.startswith('--') and line.strip('\n'):
                sql_command += line.strip('\n')
                if sql_command.endswith(';'):
                    try:
                        self.db.execute(sqlalchemy.text(sql_command))
                    finally:
                        sql_command = ''
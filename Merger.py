import pandas as pd
import mygene

mg = mygene.MyGeneInfo()


class GeneAliasRetriever:
    @staticmethod
    def get_alias(ensg_id):
        result = mg.getgenes([ensg_id], fields="name,symbol")
        if not result:
            raise SystemError
        return [ensg_id, result[0]['name'], result[0]['symbol']]


class DataFormater:

    def __init__(self, input_files, data_groups):
        self.input_files = input_files
        self.data_groups = data_groups
        self.genesIDs = []

    def merge_transcription_data(self):
        merged_files = self.get_file_content()
        transcription_data = self.format_transcription_data(merged_files)
        gene_name_data = self.get_gene_refs()
        self.write_ouput(transcription_data, gene_name_data)

    def get_gene_refs(self):
        new_df = pd.DataFrame(columns=['ID', 'Name', 'Symbol'])
        unique_ids = list(set(self.genesIDs))
        for gene_index in range(0, len(unique_ids)):
            new_df.loc[gene_index] = GeneAliasRetriever.get_alias(unique_ids[gene_index])
        return new_df

    def format_transcription_data(self, merged_files):
        new_df = pd.DataFrame(columns=['Gene', 'Tissue', 'Stage', 'Count'])

        column_names = merged_files.columns
        row_file=0
        row = 0
        line_n = len(merged_files)
        for gene_index, counts in merged_files.iterrows():
            row_file+=1
            print('Formatting line {n}/{total}'.format(n=row_file+1, total=line_n))
            self.genesIDs.append(gene_index)
            for column_index in range(0, len(column_names)):
                row+=1
                name_items = column_names[column_index].split("_")
                tissue_name = name_items[0]
                phase = name_items[1].split(".")[0]
                data_group = self.get_phase(phase)
                new_df.loc[row] = [gene_index, tissue_name, data_group, counts[column_index]]
        return new_df

    def get_phase(self, tissue_ref):
        for index, item in self.data_groups.items():
            if tissue_ref in item:
                return index
        raise SystemError

    def get_file_content(self):
        contents = []
        for input_file in self.input_files:
            contents.append(pd.read_table(input_file, index_col=0).astype(int))
        return pd.concat(contents, axis=1, sort=True)

    @staticmethod
    def write_ouput(transcription_data, gene_name_data):
        transcription_data.to_csv('transcription_data.csv')
        gene_name_data.to_csv('gene_name_data.csv')


if __name__ == '__main__':

    input_files = ['datasets/training_adult.txt', 'datasets/training_fetal.txt']
    data_groups = {
        'adult': ['adult'],
        '1t': ['9'],
        '2t': ['16-18', '22'],
    }
    data_formater = DataFormater(input_files, data_groups)
    data_formater.merge_transcription_data()


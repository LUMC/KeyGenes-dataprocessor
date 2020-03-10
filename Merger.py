import pandas as pd
import mygene

mg = mygene.MyGeneInfo()


class GeneAliasRetriever:
    @staticmethod
    def get_alias(ensg_id):
        result = mg.getgenes([ensg_id], fields="name,symbol")
        try:
            return [ensg_id, result[0]['name'], result[0]['symbol']]
        except:
            if len(result) > 2:
                print(result)
                return [ensg_id, '', result[0]['symbol']]
            else:
                return [ensg_id]



class DataFormater:

    def __init__(self, input_files, data_groups):
        self.input_files = input_files
        self.data_groups = data_groups
        self.genesIDs = []

    def merge_transcription_data(self):
        merged_files = self.get_file_content()
        self.format_transcription_data(merged_files)
        self.get_gene_refs()


    def get_gene_refs(self):
        unique_ids = list(set(self.genesIDs))
        ngnenes = len(unique_ids)
        n=0
        with open('gene_ref_data.txt', 'w') as gene_file:
            for gene_index in range(0, len(unique_ids)):
                n+=1
                print('Gene ID {current}/{total}'.format(current=n, total=ngnenes))
                gene_file.write(';'.join(GeneAliasRetriever.get_alias(unique_ids[gene_index]))+"\n")
        gene_file.close()

    def format_transcription_data(self, merged_files):
        column_names = merged_files.columns
        row_file=0
        row = 0
        line_n = len(merged_files)
        with open('transcription_data.txt', 'w') as file:
            for gene_index, counts in merged_files.iterrows():
                row_file+=1
                print('Formatting line {n}/{total}'.format(n=row_file, total=line_n))
                self.genesIDs.append(gene_index)
                for column_index in range(0, len(column_names)):
                    row+=1
                    name_items = column_names[column_index].split("_")
                    tissue_name = name_items[0]
                    phase = name_items[1].split(".")[0]
                    data_group = self.get_phase(phase)
                    file.write(';'.join([gene_index, tissue_name, data_group, str(counts[column_index])])+'\n')
        file.close()

    def get_phase(self, tissue_ref):

        for index, item in self.data_groups.items():
            print(item, tissue_ref)
            if tissue_ref in item:
                return index
        raise SystemError

    def get_file_content(self):
        contents = []
        for input_file in self.input_files:
            contents.append(pd.read_table(input_file, index_col=0).astype(int))

        return pd.concat(contents, axis=1, sort=True)


if __name__ == '__main__':

    input_files = ['datasets/training_adult.txt', 'datasets/training_fetal.txt']
    data_groups = {
        'adult': ['adult'],
        '9-weeks': ['9'],
        '16-weeks': ['16'],
        '18-weeks': ['18'],
        '22-weeks': ['22'],
    }
    data_formater = DataFormater(input_files, data_groups)
    data_formater.merge_transcription_data()


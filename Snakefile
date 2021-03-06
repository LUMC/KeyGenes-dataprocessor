import random
import string
from scripts.helpers.Database import db

# Config section
input_files = config['input']
input_file_names = [file_name.split('.')[0] for file_name in input_files]
output_dir = config['output_dir']
db_host = config['DB_HOST']
db_username = config['DB_USERNAME']
db_password = config['DB_PASSWORD']


def random_string(string_length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(string_length))
def gene_inputs(wildcards):
    files = expand("{output_dir}/expr/{file_name}_expression.tsv",output_dir=output_dir, file_name=input_file_names)
    return files

# Rule section
rule all:
    input:
         expand("{output_dir}/expr/{file_name}_expression.tsv", output_dir=output_dir, file_name=input_file_names),
         expand("{output_dir}/collections/genes.csv", output_dir=output_dir),
         expand("{output_dir}/database/material_views.txt", output_dir=output_dir)

rule create_CPM:
    input:
         "input/{file_name}.txt"
    output:
         "{output_dir}/cpm/{file_name}.tsv"
    message: "Creating CPM table for {wildcards.file_name}"
    log: "{output_dir}/logs/{file_name}_CPM.log"
    shell: "Rscript scripts/create_CPM.R "
            "{input} "
            "{output} "
            "&> {log}"

rule annotate_sex:
    input:
         "{output_dir}/cpm/{file_name}.tsv"
    output:
         "{output_dir}/sex/{file_name}.tsv"
    message: "Annotating sex for {wildcards.file_name}"
    log: "{output_dir}/logs/{file_name}_sex.log"
    shell: "python3 scripts/annotate_sex.py "
            "{input} "
            "{output} "
            "&> {log}"

rule format_expression_data:
    input:
         raw="input/{file_name}.txt",
         cpm="{output_dir}/cpm/{file_name}.tsv",
         sex="{output_dir}/sex/{file_name}.tsv"
    output:
         expression="{output_dir}/expr/{file_name}_expression.tsv"
    message: "Formatting expresson data for {wildcards.file_name}"
    log: "{output_dir}/logs/{file_name}_format_expr.log"
    shell: "python3 scripts/format_expression_data.py "
            "{input.raw} "
            "{input.cpm} "
            "{input.sex} "
            "{output.expression} "
            "&> {log}"

rule merge_data:
    input:gene_inputs
    output:
         expression="{output_dir}/collections/expression.csv",
         genes="{output_dir}/collections/genes_names.tsv",
         tissues="{output_dir}/collections/tissues.tsv",
         groups="{output_dir}/collections/groups.tsv",
    shell:
           """ cat {input} > {output.expression} && """
           """ cat {input} | awk -F ";" '{{print $1}}'| sort | uniq > {output.genes} && """
           """ cat {input} | awk -F ";" '{{print $2}}'| sort | uniq > {output.tissues} && """
           """ cat {input} | awk -F ";" '{{print $3}}'| sort | uniq > {output.groups}"""

rule get_gene_info:
    input:
        genes="{output_dir}/collections/genes_names.tsv"
    output:
        genes="{output_dir}/collections/genes.csv"
    log:
        "{output_dir}/logs/gene_info.txt"
    shell:
        "python3 scripts/get_gene_annotation.py {input.genes} {output.genes} &> {log}"

rule create_database:
    output:
        "{output_dir}/database/name.txt"
    message:
        "Creating the working database"
    log:
        "{output_dir}/logs/create_db.txt"
    shell:
        """python3 scripts/create_db.py {db_host} {db_username} {db_password} {output} &> {log}"""

rule init_db_schema:
    input:
        name="{output_dir}/database/name.txt",
        schema="files/schema.sql"
    output:
        "{output_dir}/database/schema.txt"
    log:
        "{output_dir}/logs/init_db_schema.txt"
    shell:
        """bash scripts/init_db_schema.sh {db_username} {db_password} {input.name} {input.schema} {output} &> {log}"""

rule insert_to_database:
    input:
         db="{output_dir}/database/name.txt",
         schema="{output_dir}/database/schema.txt",
         expressions="{output_dir}/collections/expression.csv",
         genes="{output_dir}/collections/genes.csv",
         tissues="{output_dir}/collections/tissues.tsv",
         groups="{output_dir}/collections/groups.tsv"
    output:
         db="{output_dir}/database/inserted.txt",
         tissue="{output_dir}/database/tissue.csv",
         stage="{output_dir}/database/stage.csv"
    log:
        "{output_dir}/logs/insert_to_database.txt"
    shell:
        """python3 scripts/insert_to_database.py {input.db} {db_host} {db_username} {db_password} {input.genes}"""
        """ {input.tissues} {input.groups}  """
        """{input.expressions} {output.db} """
         """{output.stage} {output.tissue} &> {log}"""

rule run_material_views:
    input:
         db_name="{output_dir}/database/name.txt",
         db="{output_dir}/database/inserted.txt"
    output:
         "{output_dir}/database/material_views.txt"
    log:
       "{output_dir}/logs/material_views.txt"
    shell:
        """python3 scripts/run_material_views.py""" 
        """ {input.db_name} {db_host} {db_username} {db_password} {output} &> {log}"""

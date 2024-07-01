import pandas as pd
import csv


out_dir = "output.bc-tax"
logs_dir = f'{out_dir}/logs'

bc_csv = "inputs/bc.csv"
basename = "bc"
db_basename = "ralstonia32"

onstart:
    print("------------------------------")
    print("sourmash taxonomic classification workflow")
    print("------------------------------")

onsuccess:
    print("\n--- Workflow executed successfully! ---\n")

onerror:
    print("Alas!\n")

rule all:
    input:
        f"{db_basename}.zip",
        f"{out_dir}/bc.zip",
        f"{out_dir}/bc-singleton.zip",

rule directsketch_database:
    input:
         "inputs/ralstoniadb.directsketch-input.csv"
    output:
        db_zip = "{db_basename}.zip",
        fail_csv = "{db_basename}.failed.csv"
    log: f"{logs_dir}/sketch/{{db_basename}}.directsketch.log"
    benchmark: f"{logs_dir}/sketch/{{db_basename}}.directsketch.benchmark"
    threads: 4
    params:
        param_str = "dna,k=21,k=31,k=51,scaled=5", # abund not needed for db
    shell:
        """
        sourmash scripts gbsketch -p {params.param_str} -o {output.db_zip} \
                                  --failed {output.fail_csv} {input} 2> {log}
        """

rule sketch_samples_fullfile:
    input: 
        sample_csv = bc_csv,
    output:
        f"{out_dir}/{basename}.zip",
    threads: 4
    log: f"{logs_dir}/sketch/{basename}.sketch.log"
    benchmark: f"{logs_dir}/sketch/{basename}.sketch.benchmark"
    params:
        param_str = "dna,k=21,k=31,k=51,scaled=50,abund" # abund needed for samples
    shell:
        """
        sourmash scripts manysketch -p {params.param_str} -o {output} \
                                    {input.sample_csv} -c {threads} 2> {log}
        """

# future: add "name prefix" to singleton sketches to help keep correspondance with samples?
rule sketch_samples_singleton:
    input: 
        sample_csv = bc_csv,
    output:
        f"{out_dir}/{basename}.singleton.zip",
    threads: 4
    log: f"{logs_dir}/sketch/{basename}.sketch.log"
    benchmark: f"{logs_dir}/sketch/{basename}.sketch.benchmark"
    params:
        param_str = "dna,k=21,k=31,k=51,scaled=2,abund" # abund needed for samples
    shell:
        """
        sourmash scripts manysketch -p {params.param_str} -o {output} \
                                    --singleton {input.sample_csv} -c {threads} 2> {log}
        """


rule sourmash_fastmultigather_fullfile:
    input:
        query_zip = ancient(f"{out_dir}/{{query}}.zip"),
        database  = "{db_basename}.zip",
    output:
        f"{out_dir}/fastmultigather/{{query}}-x-{{db_basename}}.k{{ksize}}-sc{{scaled}}.t{{threshold}}.gather.csv",
    resources:
        mem_mb=lambda wildcards, attempt: attempt *50000,
        time=60,
        partition="low2",
    threads: 100
    log: f"{logs_dir}/fastmultigather/{{query}}-x-{{db_basename}}.k{{ksize}}-sc{{scaled}}.t{{threshold}}.log"
    benchmark: f"{logs_dir}/fastmultigather/{{query}}-x-{{db_basename}}.k{{ksize}}-sc{{scaled}}.t{{threshold}}.benchmark"
    shell:
        """
        echo "DB: {params.db_dir}"
        echo "DB: {params.db_dir}" > {log}

        sourmash scripts fastmultigather --threshold {wildcards.threshold} \
                          --ksize {wildcards.ksize} --scaled {wildcards.scaled} \
                          {input.query_zip} -c {threads} -o {output} 2>> {log}
        """

# rule tax_annotate_lins_lingroups:
#     input:
#         gather_csv = f"{out_dir}/fastmultigather/{{basename}}-x-{{db_basename}}.k{{ksize}}-sc{{scaled}}.t{{threshold}}.gather.csv",
#         lineages = TAX_FILE,
#     output:
#         f"{out_dir}/fastmultigather/{{basename}}-x-{{db_basename}}.k{{ksize}}-sc{{scaled}}.t{{threshold}}.gather.with-lineages.csv",
#     resources:
#         mem_mb=lambda wildcards, attempt: attempt *3000,
#         partition = "low2",
#         time=240,
#     log: f"{logs_dir}/annotate/{{basename}}-x-{{db_basename}}.k{{ksize}}-sc{{scaled}}.t{{threshold}}.log"
#     benchmark: f"{logs_dir}/annotate/{{basename}}-x-{{db_basename}}.k{{ksize}}-sc{{scaled}}.t{{threshold}}.benchmark"
#     params:
#         output_dir = f"{out_dir}/fastmultigather",
#     shell:
#         """
#         sourmash tax annotate -g {input.gather_csv} \
#                               -t {input.lineages} \
#                               -o {params.output_dir} 2> {log}
#         """
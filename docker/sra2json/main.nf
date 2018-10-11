#!/usr/bin/env nextflow

params.directory = 'NCBI_SRA_Mirroring_20180801_Full'

process sra2json2s3 {
    memory "8G"
    cpus 2
    container "seandavi/omicidx"

    """
    /data/sra_directory_to_s3.sh ${params.directory}
    """
}

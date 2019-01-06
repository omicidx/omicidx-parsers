#!/usr/bin/env python
import click
import subprocess
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

@click.command()
@click.option('--s3bucket', default="s3://omicidx.cancerdatasci.org",
              help="The s3 bucket URL, including the s3:// part")
@click.option('--mirrordir',
              help="The mirror directory, like NCBI_SRA_Mirroring_20190101_Full")
def download_mirror_files(s3bucket, mirrordir):
    logger.info('getting xml files')
    subprocess.run("wget -nH -np --cut-dirs=3 -r -e robots=off {}/{}/".format(
        "http://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring",
        mirrordir), shell = True)

    logger.info('getting SRA Accessions file')
    subprocess.run("wget ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab", shell = True)

    logger.info('gzipping SRA Accessions file')
    subprocess.run("gzip -c SRA_Accessions.tab | SRA_Accessions.tab", shell=True)

    logger.info('bzipping SRA Accessions file')
    subprocess.run("bzip2 SRA_Accessions.tab", shell=True)

if __name__ == '__main__':
    download_mirror_files()
    

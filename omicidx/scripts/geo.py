import omicidx.geo.parser as op
import click
import json

@click.group()
def cli():
    pass


@cli.command()
@click.option('--gse',
              help='A single GSE accession')
def gse_to_json(gse):
    print(op.gse_to_json(gse))

@cli.command()
@click.option('--term',
              help='something like "2019/01/28:2019/02/28[PDAT]"')
@click.option("--outfile", type=click.File('w'),
              help="output file name, default is stdout")
def bulk_gse_to_json(term, outfile = '/dev/stdout'):
    op.bulk_gse_to_json(term, outfile)


@cli.command()
@click.option('--term',
              help='something like "2019/01/28:2019/02/28[PDAT]"')
@click.option("--outfile",
              help="output file name, default is stdout")
def gse_accessions(term, outfile = '/dev/stdout'):
    op.gse_accessions(term, outfile)
    
if __name__ == '__main__':
    cli()

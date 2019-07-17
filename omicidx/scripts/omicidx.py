import click
import omicidx.biosample
import sd_cloud_utils.aws.sqs as sqs
import omicidx.sra_parsers as sp

@click.group('biosample')
def biosample():
    pass

@biosample.command('downloadxml')
def biosample_download():
    """Download the biosample_set.xml.gz file"""
    omicidx.biosample.download_biosample()

@biosample.command('xml2json')
@click.argument('xmlfile')
def biosample_to_json(xmlfile):
    """Convert the biosample xml file to json"""
    omicidx.biosample.biosample_to_json(xmlfile)

@click.group('sra')
@click.pass_context
def sra():
    pass

@sra.command('download')
def sra_download():
    print('Downloading files')

@sra.command('entity2json')
@click.argument("entity")
def sra_entity_to_json(entity):
    print('convert one entity')

@sra.command('entities2json')
def sra_entities2json():
    print('convert everything at once')

@sra.command("load2postgres")
def sra_load2postgres():
    print('load and transform in postgresql')

@sra.command("load2bigquery")
def sra_load2bigquery():
    print("Load to bigquery")

@click.group(help='get SRA metadata as json')
@click.option('--config',
              help="config file")
def json(config=None):
    pass


@json.command(help='Convert SRR to json')
@click.option('--accession',
              help = "An SRA Run accession")
def srr_to_json(accession):
    models = sp.models_from_runbrowser(run_accession)
    res = {}
    for k in models.keys():
        res[k] = models[k].json()
    print(res)

    
@click.group()
def omicidx_cli():
    pass

omicidx_cli.add_command(biosample)
omicidx_cli.add_command(sra)
omicidx_cli.add_command(json)

if __name__ == '__main__':
    omicidx_cli()

import click
import omicidx.biosample


@click.group('biosample')
@click.pass_context
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

@click.group('omicidx')
def omicidx():
    pass

omicidx.add_command(biosample)
omicidx.add_command(sra)
    
if __name__ == '__main__':
    omicidx()

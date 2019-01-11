import click
from omicidx.biosample import download_biosample
from omicidx.biosample import biosample_to_json

@click.group()
def omicidx():
    pass

@omicidx.group("biosample")
def biosample():
    pass

@biosample.command('download')
def biosample_download():
    download_biosample()

@biosample.command('xml2json')
@click.argument('xmlfile')
def biosample_to_json(xmlfile):
    biosample_to_json(xmlfile)

if __name__ == '__main__':
    omicidx()

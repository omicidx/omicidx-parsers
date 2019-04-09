from ../geometa import Base
import click

@click.command()
@click.option('--schema')
@click.option('--url')
def make_schema(url, schema = "public"):
    engine = create_engine(url)
    

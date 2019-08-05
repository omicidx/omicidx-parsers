import pytest

import omicidx.sra_parsers as s

EXAMPLE_SRR = 'SRR001237'

@pytest.fixture
def example_runbrowser_model():
    """Return a populated pydantic model dict from runbrowser"""
    return s.models_from_runbrowser(EXAMPLE_SRR)

def test_runbrowser_result_has_keys(example_runbrowser_model):
    """runbrowser model has four keys"""
    assert len(example_runbrowser_model.keys())==4
    for i in 'run sample study experiment'.split():
        assert i in example_runbrowser_model.keys()

def test_runbrowser_run_has_qualities(example_runbrowser_model):
    run = example_runbrowser_model['run']
    assert len(run.qualities) > 0

def test_runbrowser_run_accession_matches(example_runbrowser_model):
    run = example_runbrowser_model['run']
    assert run.accession == EXAMPLE_SRR




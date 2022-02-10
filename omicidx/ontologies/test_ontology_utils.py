import pytest
from .utils import (get_all_ontologies_from_obo_library,
                    ontology_from_obo_library)
import pronto


def test_ids_in_get_all_ontologies_from_obo_library():
    ret = get_all_ontologies_from_obo_library()
    for i in ret:
        assert 'id' in i


def test_get_an_ontology():
    cl = ontology_from_obo_library('cl.owl')
    assert 'CL:0000900' in cl
    assert isinstance(cl, pronto.Ontology)
    cl_inst = cl['CL:0000900']
    assert isinstance(cl_inst.name, str)

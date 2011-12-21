from django.test import TestCase
from tardis.tardis_portal.models import Dataset, Schema
from tardis.apps.atomimport.atom_ingest import AtomParser, AtomImportSchemas
from flexmock import flexmock, flexmock_teardown
import os, inspect

class SchemaTestCase(TestCase):

    def testHasSchemas(self):
        assert inspect.ismethod(AtomImportSchemas.get_schemas)
        assert len(AtomImportSchemas.get_schemas()) > 0
        for schema in AtomImportSchemas.get_schemas():
            assert isinstance(schema, Schema)
            assert schema.namespace.find(AtomImportSchemas.BASE_NAMESPACE) > -1
            assert schema.name != None
            assert schema.type != None
            assert schema.subtype != None
            assert schema.id != None

class ProcessorTestCase(TestCase):

    def testParserReadsAtom(self):
        f = open(os.path.dirname(__file__)+'/atom_test/datasets.atom')
        atom_doc = f.read()
        parser = AtomParser(atom_doc)
        assert inspect.ismethod(parser.get_datasets)
        datasets = parser.get_datasets()
        assert isinstance(datasets, list)
        assert len(datasets) == 2
        for dataset in datasets:
            assert isinstance(dataset, Dataset)
            #datafiles = parser.get_datafiles(dataset)
            #assert isinstance(datafiles, list)
            #assert len(datafiles) == 2


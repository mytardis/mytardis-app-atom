from django.test import TestCase
from tardis.tardis_portal.models import Dataset, Schema
from tardis.apps.atomimport.atom_ingest import AtomWalker, AtomImportSchemas
from flexmock import flexmock, flexmock_teardown
from SimpleHTTPServer import SimpleHTTPRequestHandler
import BaseHTTPServer, os, inspect, SocketServer, threading


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

    class TestWebServer:
        '''
        Utility class for running a test web server with a given handler.
        '''

        class ThreadedTCPServer(SocketServer.ThreadingMixIn, \
                                BaseHTTPServer.HTTPServer):
            pass

        def __init__(self):
            self.handler = SimpleHTTPRequestHandler

        def start(self):
            server = self.ThreadedTCPServer(('127.0.0.1', self.getPort()),
                                            self.handler)
            server_thread = threading.Thread(target=server.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            self.server = server
            return server.socket.getsockname()

        def getUrl(self):
            return 'http://%s:%d/' % self.server.socket.getsockname()

        @classmethod
        def getPort(cls):
            return 4272

        def stop(self):
            self.server.shutdown()


    def setUp(self):
        self.priorcwd = os.getcwd()
        os.chdir(os.path.dirname(__file__)+'/atom_test')
        self.server = self.TestWebServer()
        self.server.start()
        pass

    def tearDown(self):
        os.chdir(self.priorcwd)
        self.server.stop()
        flexmock_teardown()

    def testWalkerFollowsAtomLinks(self):
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (self.TestWebServer.getPort()))
        assert inspect.ismethod(parser.datasets)
        print parser.datasets()
        num_datasets = reduce(lambda a,b: a+1, parser.datasets(), 0)
        assert num_datasets == 4
        chronologically_asc = reduce(self._check_chronological_order,\
                                     parser.datasets(), None)
        assert chronologically_asc
        for dataset in parser.datasets():
            assert isinstance(dataset, Dataset)
            #datafiles = parser.get_datafiles(dataset)
            #assert isinstance(datafiles, list)
            #assert len(datafiles) == 2

    @staticmethod
    def _check_chronological_order(dataset_a, dataset_b):
        '''
        This function checks that the Atom data sets are in chronological order.
        IT IS DEPENDENT ON THE TEST DATA! It does not apply to all Atom
        documents in general.
        '''
        if dataset_a == False:
            return False
        if dataset_a == None or dataset_a.description < dataset_b.description:
            return dataset_b
        return False

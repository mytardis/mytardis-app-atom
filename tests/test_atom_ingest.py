from django.test import TestCase
from tardis.tardis_portal.models import Dataset, Schema
from tardis.apps.atomimport.atom_ingest import AtomPersister, AtomWalker, AtomImportSchemas
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

        class QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
            '''
            Simple subclass that only prints output to STDOUT, not STDERR
            '''
            def log_message(self, msg, *args):
                print msg % args

        class ThreadedTCPServer(SocketServer.ThreadingMixIn, \
                                BaseHTTPServer.HTTPServer):
            pass

        def __init__(self):
            self.handler = self.QuietSimpleHTTPRequestHandler

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


    @classmethod
    def setUpClass(cls):
        cls.priorcwd = os.getcwd()
        os.chdir(os.path.dirname(__file__)+'/atom_test')
        cls.server = cls.TestWebServer()
        cls.server.start()
        pass

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls.priorcwd)
        cls.server.stop()

    def tearDown(self):
        flexmock_teardown()

    def testWalkerFollowsAtomLinks(self):
        '''
        Test that the walker follows links.
        '''
        # We build a persister which says all entries are new.
        persister = flexmock(AtomPersister())
        persister.should_receive('is_new').with_args(object, object)\
            .and_return(True).times(4)
        persister.should_receive('process').with_args(object, object)\
            .times(4)
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (self.TestWebServer.getPort()),
                            persister)
        assert inspect.ismethod(parser.ingest)
        parser.ingest()

    def testWalkerProcessesEntriesInCorrectOrder(self):
        '''
        Test that the walker processes the entries in the revese order that it
        finds them.
        '''
        checked_entries = []
        processed_entries = []
        # We build a persister which says all entries are new.
        persister = flexmock(AtomPersister())
        # Grab the checked entry and return true
        persister.should_receive('is_new').with_args(object, object)\
            .replace_with(lambda feed, entry: checked_entries.append(entry) or True)
        # Grab the processed entry
        persister.should_receive('process').with_args(object, object)\
            .replace_with(lambda feed, entry: processed_entries.append(entry))
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (self.TestWebServer.getPort()),
                            persister)
        parser.ingest()
        # We should have checked four entries, chronologically decendent
        assert len(checked_entries) == 4
        checked_backwards = reduce(self._check_chronological_asc_order,\
                                     reversed(checked_entries), None)
        assert checked_backwards
        # We should have processed four entries, chronologically ascendent
        assert len(processed_entries) == 4
        processed_forwards = reduce(self._check_chronological_asc_order,\
                                     processed_entries, None)
        assert processed_forwards

    def testWalkerOnlyIngestsNewEntries(self):
        '''
        Test that the walker will stop when it gets to an entry that isn't new.
        '''
        # We build a persister which says there are three entries
        # that aren't in the repository.
        persister = flexmock(AtomPersister())
        persister.should_receive('is_new').with_args(object, object)\
            .and_return(True, True, True, False).one_by_one.times(4)
        persister.should_receive('process').with_args(object, object).times(3)
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (self.TestWebServer.getPort()),
                            persister)
        parser.ingest()
        # We build a persister which says there are two entries
        # that aren't in the repository.
        persister = flexmock(AtomPersister())
        persister.should_receive('is_new').with_args(object, object)\
            .and_return(True, True, False, False).one_by_one.times(4)
        persister.should_receive('process').with_args(object, object).times(2)
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (self.TestWebServer.getPort()),
                            persister)
        parser.ingest()
        # We build a persister which says there is one entry
        # that isn't in the repository.
        persister = flexmock(AtomPersister())
        persister.should_receive('is_new').with_args(object, object)\
            .and_return(True, False, False, False).one_by_one.times(2)
        persister.should_receive('process').with_args(object, object).times(1)
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (self.TestWebServer.getPort()),
                            persister)
        parser.ingest()

    @staticmethod
    def _check_chronological_asc_order(entry_a, entry_b):
        '''
        This function checks that the Atom entries sets are in chronological
        order.
        '''
        if entry_a == False:
            return False
        if entry_a == None or entry_a.updated_parsed < entry_b.updated_parsed:
            return entry_b
        return False

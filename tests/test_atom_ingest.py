from django.conf import settings
from django.test import TestCase
from compare import expect
import iso8601
from tardis.tardis_portal.ParameterSetManager import ParameterSetManager
from tardis.tardis_portal.models import Experiment, Dataset, Dataset_File, \
    Location, Schema, User
from ..atom_ingest import AtomPersister, AtomWalker, \
    AtomImportSchemas, __file__ as atom_ingest_file
import feedparser
from urlparse import urlparse
from os import path
from nose.tools import ok_, eq_
from flexmock import flexmock, flexmock_teardown
from SimpleHTTPServer import SimpleHTTPRequestHandler
import BaseHTTPServer, base64, os, inspect, SocketServer, threading, urllib2

from tardis.tardis_portal.tests.test_fetcher import TestWebServer

class AbstractAtomServerTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.priorcwd = os.getcwd()
        os.chdir(os.path.dirname(__file__)+'/atom_test')
        cls.server = TestWebServer()
        cls.server.start()

        Location.force_initialize()
        Location.load_location({
            'name': 'test-atom',
            'transfer_provider': 'http',
            'url': 'http://localhost:4272/files/',
            'type': 'external',
            'priority': 10})
        Location.load_location({
            'name': 'test-atom2', 
            'transfer_provider': 'http',
            'url': 'http://mydatagrabber.cmm.uq.edu.au/files',
            'type': 'external',
            'priority': 10})

        files = path.realpath(path.join(path.dirname(__file__), 
                                        'atom_test', 'files'))
        Location.load_location({
            'name': 'test-atom3',
            'transfer_provider': 'local',
            'url': 'file://' + files,
            'type': 'external',
            'priority': 10})

    @classmethod
    def tearDownClass(cls):
        Location.objects.get(name='test-atom').delete()
        Location.objects.get(name='test-atom2').delete()
        Location.objects.get(name='test-atom3').delete()
        os.chdir(cls.priorcwd)
        cls.server.stop()



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



class PersisterTestCase(AbstractAtomServerTestCase):

    def testPersisterRecognisesNewEntries(self):
        feed, entry = self._getTestEntry()
        p = AtomPersister()
        ok_(p.is_new(feed, entry), "Entry should not already be in DB")
        ok_(p.is_new(feed, entry), "New entries change only when processed")
        dataset = p.process(feed, entry)
        ok_(dataset != None)
        eq_(p.is_new(feed, entry), False, "(processed => !new) != True")


    def testPersisterCreatesDatafiles(self):
        feed, entry = self._getTestEntry()
        # Frig the enclosure hrefs so that we can 'fetch' the files
        # synchronously using 'file://' urls
        dir = path.realpath(path.join(path.dirname(__file__), 
                                      'atom_test', 'files') )
        entry['links'][0]['href'] = 'file://' + path.join(dir, 'abcd0001.tif')
        entry['links'][1]['href'] = 'file://' + path.join(dir, 'abcd0001.txt')
        p = AtomPersister(async_copy=False)
        ok_(p.is_new(feed, entry), "Entry should not already be in DB")
        dataset = p.process(feed, entry)
        datafiles = dataset.dataset_file_set.all()
        eq_(len(datafiles), 2)
        image = dataset.dataset_file_set.get(filename='abcd0001.tif')
        # No mimetype specified, so should auto-detect
        eq_(image.mimetype, 'image/tiff')
        image_url = image.get_preferred_replica().url
        ok_(urlparse(image_url).scheme == '', "Not local: %s" % image_url)
        file_path = path.join(settings.MEDIA_ROOT, image_url)
        ok_(path.isfile(file_path), "File does not exist: %s" % file_path)
        image = dataset.dataset_file_set.get(filename='metadata.txt')
        eq_(image.mimetype, 'text/plain')


    def testPersisterUsesTitleElements(self):
        feed, entry = self._getTestEntry()
        p = AtomPersister()
        dataset = p.process(feed, entry)
        eq_(dataset.description, entry.title)


    def testPersisterUsesAuthorNameAsUsername(self):
        # Create user to associate with dataset
        user = User(username="tatkins")
        user.save()
        feed, entry = self._getTestEntry()
        p = AtomPersister()
        dataset = p.process(feed, entry)
        eq_(dataset.get_first_experiment().created_by, user)

    def testPersisterHandlesSpacesInUsername(self):
        # Get entry and use a name with a space in it
        feed, entry = self._getTestEntry()
        entry.author_detail.name = 'Tommy Atkins'
        # Create user to associate with dataset
        user = User(username="Tommy_Atkins")
        user.save()
        p = AtomPersister()
        dataset = p.process(feed, entry)
        eq_(dataset.get_first_experiment().created_by, user)

    def testPersisterPrefersAuthorEmailToMatchUser(self):
        # Create user to associate with dataset
        user = User(username="tatkins")
        user.save()
        # Create user to associate with dataset
        user2 = User(username="tommy", email='tatkins@example.test')
        user2.save()
        feed, entry = self._getTestEntry()
        p = AtomPersister()
        dataset = p.process(feed, entry)
        eq_(dataset.get_first_experiment().created_by, user2)

    def testPersisterStoresEntryMetadata(self):
        # Create user to associate with dataset
        user = User(username="tatkins")
        user.save()
        feed, entry = self._getTestEntry()
        p = AtomPersister()
        dataset = p.process(feed, entry)
        parameterset = dataset.getParameterSets() \
                              .get(schema=AtomImportSchemas. \
                                          get_schema(Schema.DATASET))
        expect(parameterset) != None
        psm = ParameterSetManager(parameterset)
        expect(psm.get_param('EntryID').get()).to_equal(entry.id)
        expect(psm.get_param('Updated').name.isDateTime()).to_be_truthy()
        # Compare against non-timezoned update time
        expect(psm.get_param('Updated', True)) \
            .to_equal(iso8601.parse_date(entry.updated))


    def testPersisterUsesExperimentMetadata(self):
        # Build a persister with "make_local_copy" mocked out
        # (file transfer isn't part of this test)
        persister = flexmock(AtomPersister())
        persister.should_receive('make_local_copy').times(8)

        doc = feedparser.parse('datasets2.atom')
        save1 = settings.REQUIRE_DATAFILE_CHECKSUMS
        save2 = settings.REQUIRE_DATAFILE_SIZES
        try:
            settings.REQUIRE_DATAFILE_CHECKSUMS = False
            settings.REQUIRE_DATAFILE_SIZES = False
            # Process the first set of entries
            for entry in reversed(doc.entries):
                persister.process(doc.feed, entry)
        finally:
            settings.REQUIRE_DATAFILE_CHECKSUMS = save1
            settings.REQUIRE_DATAFILE_SIZES = save2
            
        # We processed 4 datafiles
        eq_(Dataset_File.objects.count(), 4)
        # We processed 2 datasets
        eq_(Dataset.objects.count(), 2)
        # This part has a single user
        eq_(User.objects.count(), 1)
        # This part is untagged, so there should only be a single experiment
        eq_(Experiment.objects.count(), 1)

        doc = feedparser.parse('datasets.atom')
        # Process the rest of the entries
        for entry in reversed(doc.entries):
            persister.process(doc.feed, entry)
        # Whe should now have 8 datafiles
        eq_(Dataset_File.objects.count(), 8)
        # Whe should now have 4 datasets
        eq_(Dataset.objects.count(), 4)
        # Whe should now have two users
        eq_(User.objects.count(), 2)
        # This part is tagged as two experiements, so there should be three now
        eq_(Experiment.objects.count(), 3)

    def testPersisterHandlesMultipleDatafiles(self):
        doc = feedparser.parse('datasets.atom')
        p = AtomPersister()
        for feed, entry in map(lambda x: (doc.feed, x), doc.entries):
            ok_(p.is_new(feed, entry))
            p.process(feed, entry)
            eq_(p.is_new(feed, entry), False, "(processed => !new) != True")


    def _getTestEntry(self, atom_file='datasets.atom'):
        doc = feedparser.parse(atom_file)
        entry = doc.entries.pop()
        # Check we're looking at the right entry
        assert entry.id.endswith('BEEFCAFE0001')
        return (doc.feed, entry)



class WalkerTestCase(AbstractAtomServerTestCase):

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
        walker = AtomWalker('http://localhost:%d/datasets.atom' %
                            (TestWebServer.getPort()),
                            persister)
        ok_(inspect.ismethod(walker.ingest),"Walker must have an ingest method")
        walker.ingest()


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
                            (TestWebServer.getPort()),
                            persister)
        parser.ingest()
        # We should have checked four entries, chronologically decendent
        eq_(len(checked_entries), 4)
        checked_backwards = reduce(self._check_chronological_asc_order,\
                                     reversed(checked_entries), None)
        ok_(checked_backwards, "Entries aren't parsed in reverse chronological order")
        # We should have processed four entries, chronologically ascendent
        eq_(len(processed_entries), 4)
        processed_forwards = reduce(self._check_chronological_asc_order,\
                                     processed_entries, None)
        ok_(processed_forwards, "Entries aren't parsed in chronological order")


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
                            (TestWebServer.getPort()),
                            persister)
        parser.ingest()
        # We build a persister which says there are two entries
        # that aren't in the repository.
        persister = flexmock(AtomPersister())
        persister.should_receive('is_new').with_args(object, object)\
            .and_return(True, True, False, False).one_by_one.times(4)
        persister.should_receive('process').with_args(object, object).times(2)
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (TestWebServer.getPort()),
                            persister)
        parser.ingest()
        # We build a persister which says there is one entry
        # that isn't in the repository.
        persister = flexmock(AtomPersister())
        persister.should_receive('is_new').with_args(object, object)\
            .and_return(True, False, False, False).one_by_one.times(2)
        persister.should_receive('process').with_args(object, object).times(1)
        parser = AtomWalker('http://localhost:%d/datasets.atom' %
                            (TestWebServer.getPort()),
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



import feedparser
from posixpath import basename
from tardis.tardis_portal.ParameterSetManager import ParameterSetManager
from tardis.tardis_portal.models import Dataset, DatasetParameter,\
    Experiment, ExperimentParameter, ParameterName, Schema, User
from django.conf import settings
import urllib2

class AtomImportSchemas:

    BASE_NAMESPACE = 'http://mytardis.org/schemas/atom-import'


    @classmethod
    def get_schemas(cls):
        return Schema.objects.filter(namespace__startswith=cls.BASE_NAMESPACE)


    @classmethod
    def get_schema(cls, schema_type=Schema.DATASET):
        return Schema.objects.get(namespace__startswith=cls.BASE_NAMESPACE,
                                  type=schema_type)



class AtomPersister:

    PARAM_ENTRY_ID = 'EntryID'
    PARAM_EXPERIMENT_ID = 'ExperimentID'


    def is_new(self, feed, entry):
        '''
        :param feed: Feed context for entry
        :param entry: Entry to check
        returns a boolean
        '''
        try:
            self._get_dataset(feed, entry)
            return False
        except Dataset.DoesNotExist:
            return True


    def _get_dataset(self, feed, entry):
        try:
            param_name = ParameterName.objects.get(name=self.PARAM_ENTRY_ID,
                                                   schema=AtomImportSchemas.get_schema())
            parameter = DatasetParameter.objects.get(name=param_name,
                                                     string_value=entry.id)
        except DatasetParameter.DoesNotExist:
            raise Dataset.DoesNotExist
        return parameter.parameterset.dataset



    def _create_entry_id_parameter_set(self, dataset, entryId):
        namespace = AtomImportSchemas.get_schema(Schema.DATASET).namespace
        mgr = ParameterSetManager(parentObject=dataset, schema=namespace)
        mgr.new_param(self.PARAM_ENTRY_ID, entryId)


    def _create_experiment_id_parameter_set(self, experiment, experimentId):
        namespace = AtomImportSchemas.get_schema(Schema.EXPERIMENT).namespace
        mgr = ParameterSetManager(parentObject=experiment, schema=namespace)
        mgr.new_param(self.PARAM_EXPERIMENT_ID, experimentId)


    def _get_user_from_entry(self, entry):
        if entry.author_detail.email != None:
            try:
                return User.objects.get(email=entry.author_detail.email)
            except User.DoesNotExist:
                pass
        try:
            return User.objects.get(username=entry.author_detail.name)
        except User.DoesNotExist:
            pass
        user = User(username=entry.author_detail.name)
        user.save()
        return user


    def process_enclosure(self, dataset, enclosure):
        filename = getattr(enclosure, 'title', basename(enclosure.href))
        datafile = dataset.dataset_file_set.create(url=enclosure.href, \
                                                   filename=filename)
        try:
            datafile.mimetype = enclosure.mime
        except AttributeError:
            pass
        datafile.save()
        self.make_local_copy(datafile)


    def process_media_content(self, dataset, media_content):
        try:
            filename = basename(media_content['url'])
        except AttributeError:
            return
        datafile = dataset.dataset_file_set.create(url=media_content['url'], \
                                                   filename=filename)
        try:
            datafile.mimetype = media_content['type']
        except IndexError:
            pass
        datafile.save()
        self.make_local_copy(datafile)


    def make_local_copy(self, datafile):
        from tardis.apps.atomimport.tasks import make_local_copy
        make_local_copy.delay(datafile)


    def _get_experiment_details(self, entry):
        try:
            return (entry.gphoto_albumid, entry.gphoto_albumtitle)
        except AttributeError:
            return (entry.id, entry.title)


    def _get_experiment(self, entry, user):
        experimentId, title = self._get_experiment_details(entry)
        try:
            try:
                param_name = ParameterName.objects.\
                    get(name=self.PARAM_EXPERIMENT_ID, \
                        schema=AtomImportSchemas.get_schema(Schema.EXPERIMENT))
                parameter = ExperimentParameter.objects.\
                    get(name=param_name, string_value=experimentId)
            except ExperimentParameter.DoesNotExist:
                raise Experiment.DoesNotExist
            return parameter.parameterset.experiment
        except Experiment.DoesNotExist:
            experiment = Experiment(title=title, created_by=user)
            experiment.save()
            self._create_experiment_id_parameter_set(experiment, experimentId)
            return experiment


    def process(self, feed, entry):
        user = self._get_user_from_entry(entry)
        # Create dataset if necessary
        try:
            dataset = self._get_dataset(feed, entry)
        except Dataset.DoesNotExist:
            experiment = self._get_experiment(entry, user)
            dataset = experiment.dataset_set.create(description=entry.title)
            dataset.save()
            self._create_entry_id_parameter_set(dataset, entry.id)
            for enclosure in getattr(entry, 'enclosures', []):
                self.process_enclosure(dataset, enclosure)
            for media_content in getattr(entry, 'media_content', []):
                self.process_media_content(dataset, media_content)
        return dataset



class AtomWalker:


    def __init__(self, root_doc, persister = AtomPersister()):
        self.root_doc = root_doc
        self.persister = persister


    @staticmethod
    def get_credential_handler():
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        try:
            for url, username, password in settings.ATOM_FEED_CREDENTIALS:
                passman.add_password(None, url, username, password)
        except AttributeError:
            # We may not have settings.ATOM_FEED_CREDENTIALS
            pass
        handler = urllib2.HTTPBasicAuthHandler(passman)
        handler.handler_order = 490
        return handler


    @staticmethod
    def _get_next_href(doc):
        try:
            links = filter(lambda x: x.rel == 'next', doc.feed.links)
            if len(links) < 1:
                return None
            return links[0].href
        except AttributeError:
            # May not have any links to filter
            return None


    def ingest(self):
        for feed, entry in self.get_entries():
            self.persister.process(feed, entry)


    def get_entries(self):
        '''
        returns list of (feed, entry) tuples
        '''
        doc = self.fetch_feed(self.root_doc)
        entries = []
        while True:
            if doc == None:
                break
            new_entries = filter(lambda entry: self.persister.is_new(doc.feed, entry), doc.entries)
            entries.extend(map(lambda entry: (doc.feed, entry), new_entries))
            next_href = self._get_next_href(doc)
            # Stop if the filter found an existing entry or no next
            if len(new_entries) != len(doc.entries) or next_href == None:
                break
            doc = self.fetch_feed(next_href)
        return reversed(entries)


    def fetch_feed(self, url):
        return feedparser.parse(url, handlers=[self.get_credential_handler()])


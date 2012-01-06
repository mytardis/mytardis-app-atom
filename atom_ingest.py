import feedparser
from posixpath import basename
from celery.task import task
from tardis.tardis_portal.staging import write_uploaded_file_to_dataset
from tardis.tardis_portal.ParameterSetManager import ParameterSetManager
from tardis.tardis_portal.models import Dataset, DatasetParameter,\
    Experiment, ParameterName, Schema, User
from urllib2 import urlopen


@task
def make_local_copy(datafile):
    f = urlopen(datafile.url)
    print f.info()
    f_loc = write_uploaded_file_to_dataset(datafile.dataset, f, datafile.filename)
    datafile.url = 'tardis:/' + f_loc
    datafile.save()


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


    @classmethod
    def _create_id_parameter_set(self, dataset, entry):
        namespace = AtomImportSchemas.get_schema().namespace
        mgr = ParameterSetManager(parentObject=dataset, schema=namespace)
        mgr.new_param(self.PARAM_ENTRY_ID, entry.id)


    def process(self, feed, entry):
        # Create user to associate with dataset
        username = "feedimportuser"
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            user = User(username=username)
            user.save()
        # Create dataset if necessary
        try:
            dataset = self._get_dataset(feed, entry)
        except Dataset.DoesNotExist:
            try:
                experiment = Experiment.objects.get(title=feed.id)
            except Experiment.DoesNotExist:
                experiment = Experiment(title=feed.id, created_by=user)
                experiment.save()
            dataset = experiment.dataset_set.create(description=entry.title)
            dataset.save()
            self._create_id_parameter_set(dataset, entry)
            for enclosure in entry.enclosures:
                filename = getattr(enclosure, 'title', basename(enclosure.href))
                datafile = dataset.dataset_file_set.create(url=enclosure.href,
                                                           filename=filename)
                datafile.mimetype = getattr(enclosure,\
                                            'mime', 'application/octet-stream')
                datafile.save()
                make_local_copy.delay(datafile)
        return dataset



class AtomWalker:


    def __init__(self, root_doc, persister = AtomPersister()):
        self.root_doc = root_doc
        self.persister = persister


    @staticmethod
    def _get_next_href(doc):
        links = filter(lambda x: x.rel == 'next', doc.feed.links)
        if len(links) < 1:
            return None
        return links[0].href


    def ingest(self):
        for feed, entry in self.get_entries():
            self.persister.process(feed, entry)


    def get_entries(self):
        '''
        returns list of (feed, entry) tuples
        '''
        doc = feedparser.parse(self.root_doc)
        entries = []
        while True:
            new_entries = filter(lambda entry: self.persister.is_new(doc.feed, entry), doc.entries)
            entries.extend(map(lambda entry: (doc.feed, entry), new_entries))
            next_href = self._get_next_href(doc)
            # Stop if the filter found an existing entry or no next
            if len(new_entries) != len(doc.entries) or next_href == None:
                break
            doc = feedparser.parse(next_href)
        return reversed(entries)


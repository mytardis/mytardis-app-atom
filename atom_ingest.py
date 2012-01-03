import feedparser
from posixpath import basename
from tardis.tardis_portal.models import Experiment, Dataset, Schema, User

class AtomImportSchemas:

    BASE_NAMESPACE = 'http://mytardis.org/schemas/atom-import'

    @classmethod
    def get_schemas(cls):
        return Schema.objects.filter(namespace__startswith=cls.BASE_NAMESPACE)


class AtomPersister:

    def is_new(self, feed, entry):
        '''
        :param feed: Feed context for entry
        :param entry: Entry to check
        returns a boolean
        '''
        try:
            feed = Experiment.objects.get(title=feed.id)
        except Experiment.DoesNotExist:
            return True
        feed.dataset_set.get(description=entry.id)
        return False

    def process(self, feed, entry):
        username = "feedimportuser"
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            user = User(username=username)
            user.save()
        experiment = Experiment(title=feed.id, created_by=user)
        experiment.save()
        dataset = experiment.dataset_set.create(description=entry.id)
        dataset.save()
        for enclosure in entry.enclosures:
            filename = getattr(enclosure, 'title', basename(enclosure.href))
            datafile = dataset.dataset_file_set.create(url=enclosure.href,
                                                       filename=filename)
            datafile.mimetype = getattr(enclosure,\
                                        'mime', 'application/octet-stream')
            datafile.save()
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


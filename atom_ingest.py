import feedparser
from tardis.tardis_portal.models import Dataset, Schema

class AtomImportSchemas:

    BASE_NAMESPACE = 'http://mytardis.org/schemas/atom-import'

    @classmethod
    def get_schemas(cls):
        return Schema.objects.filter(namespace__startswith=cls.BASE_NAMESPACE)


class AtomPersister:

    def is_new(self, feed, entry):
        '''
        @feed_id: str
        @entry_id: str
        returns a boolean
        '''
        return True

    def process(self, feed, entry):
        pass

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
            if next_href == None:
                break
            doc = feedparser.parse(next_href)
        return reversed(entries)


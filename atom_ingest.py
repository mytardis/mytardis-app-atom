import feedparser
from tardis.tardis_portal.models import Dataset, Schema

class AtomImportSchemas:

    BASE_NAMESPACE = 'http://mytardis.org/schemas/atom-import'

    @classmethod
    def get_schemas(cls):
        return Schema.objects.filter(namespace__startswith=cls.BASE_NAMESPACE)

class AtomWalker:

    def __init__(self,root_doc):
        self.root_doc = root_doc

    @staticmethod
    def _get_next_href(doc):
        links = filter(lambda x: x.rel == 'next', doc.feed.links)
        if len(links) < 1:
            return None
        return links[0].href

    def datasets(self):
        doc = feedparser.parse(self.root_doc)
        datasets = []
        while True:
            datasets.extend(map(self._entry_processor, doc.entries))
            next = self._get_next_href(doc)
            print next
            if next == None:
                break
            doc = feedparser.parse(next)
        return datasets

    def _entry_processor(self, entry):
        d = Dataset()
        d.description = entry.title
        return d
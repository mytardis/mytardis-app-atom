import feedparser
from tardis.tardis_portal.models import Dataset, Schema

class AtomImportSchemas:

    BASE_NAMESPACE = 'http://mytardis.org/schemas/atom-import'

    @classmethod
    def get_schemas(cls):
        return Schema.objects.filter(namespace__startswith=cls.BASE_NAMESPACE)

class AtomParser:

    def __init__(self,doc):
        self.doc = doc

    def get_datasets(self):
        d = feedparser.parse(self.doc)
        datasets = map(self._entry_processor, d.entries)
        return datasets

    def _entry_processor(self, entry):
        d = Dataset()
        d.description = entry.title
        return d
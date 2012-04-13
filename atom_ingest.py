import feedparser
import iso8601
from posixpath import basename
from tardis.tardis_portal.auth.localdb_auth import django_user
from tardis.tardis_portal.ParameterSetManager import ParameterSetManager
from tardis.tardis_portal.models import Dataset, DatasetParameter,\
    Experiment, ExperimentACL, ExperimentParameter, ParameterName, Schema, \
    User, UserProfile
from django.conf import settings
import urllib2

import logging
logger = logging.getLogger(__name__)

class AtomImportSchemas:

    BASE_NAMESPACE = 'http://mytardis.org/schemas/atom-import'


    @classmethod
    def get_schemas(cls):
        cls._load_fixture_if_necessary();
        return cls._get_all_schemas();

    @classmethod
    def get_schema(cls, schema_type=Schema.DATASET):
        cls._load_fixture_if_necessary();
        return Schema.objects.get(namespace__startswith=cls.BASE_NAMESPACE,
                                  type=schema_type)

    @classmethod
    def _load_fixture_if_necessary(cls):
        if (cls._get_all_schemas().count() == 0):
            from django.core.management import call_command
            call_command('loaddata', 'atom_ingest_schema')

    @classmethod
    def _get_all_schemas(cls):
        return Schema.objects.filter(namespace__startswith=cls.BASE_NAMESPACE)



class AtomPersister:

    PARAM_ENTRY_ID = 'EntryID'
    PARAM_EXPERIMENT_ID = 'ExperimentID'
    PARAM_UPDATED = 'Updated'
    PARAM_EXPERIMENT_TITLE = 'ExperimentTitle'


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


    def _create_entry_parameter_set(self, dataset, entryId, updated):
        namespace = AtomImportSchemas.get_schema(Schema.DATASET).namespace
        mgr = ParameterSetManager(parentObject=dataset, schema=namespace)
        mgr.new_param(self.PARAM_ENTRY_ID, entryId)
        mgr.new_param(self.PARAM_UPDATED, iso8601.parse_date(updated))


    def _create_experiment_id_parameter_set(self, experiment, experimentId):
        namespace = AtomImportSchemas.get_schema(Schema.EXPERIMENT).namespace
        mgr = ParameterSetManager(parentObject=experiment, schema=namespace)
        mgr.new_param(self.PARAM_EXPERIMENT_ID, experimentId)


    def _get_user_from_entry(self, entry):
        try:
            if entry.author_detail.email != None:
                return User.objects.get(email=entry.author_detail.email)
        except (User.DoesNotExist, AttributeError):
            pass
        # Handle spaces in name
        username_ = entry.author_detail.name.strip().replace(" ", "_")
        try:
            return User.objects.get(username=username_)
        except User.DoesNotExist:
            pass
        user = User(username=username_)
        user.save()
        UserProfile(user=user).save()
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
        from .tasks import make_local_copy
        make_local_copy.delay(datafile)


    def _get_experiment_details(self, entry, user):
        try:
            # Google Picasa attributes
            if entry.has_key('gphoto_albumid'):
                return (entry.gphoto_albumid,
                        entry.gphoto_albumtitle,
                        Experiment.PUBLIC_ACCESS_FULL)
            # Standard category handling
            experimentId = None
            title = None
            # http://packages.python.org/feedparser/reference-entry-tags.html
            for tag in entry.tags:
                if tag.scheme.endswith(self.PARAM_EXPERIMENT_ID):
                    experimentId = tag.term
                if tag.scheme.endswith(self.PARAM_EXPERIMENT_TITLE):
                    title = tag.term
            if (experimentId != None and title != None):
                return (experimentId, title, Experiment.PUBLIC_ACCESS_NONE)
        except AttributeError:
            pass
        return (user.username+"-default",
                "Uncategorized Data",
                Experiment.PUBLIC_ACCESS_NONE)


    def _get_experiment(self, entry, user):
        experimentId, title, public_access = \
            self._get_experiment_details(entry, user)
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
            experiment = Experiment(title=title,
                                    created_by=user,
                                    public_access=public_access)
            experiment.save()
            self._create_experiment_id_parameter_set(experiment, experimentId)
            acl = ExperimentACL(experiment=experiment,
                    pluginId=django_user,
                    entityId=user.id,
                    canRead=True,
                    canWrite=True,
                    canDelete=True,
                    isOwner=True,
                    aclOwnershipType=ExperimentACL.OWNER_OWNED)
            acl.save()
            return experiment


    def process(self, feed, entry):
        user = self._get_user_from_entry(entry)
        # Create dataset if necessary
        try:
            dataset = self._get_dataset(feed, entry)
        except Dataset.DoesNotExist:
            experiment = self._get_experiment(entry, user)
            dataset = experiment.dataset_set.create(description=entry.title)
            logger.debug('Creating new dataset: %s' % entry.title)
            dataset.save()
            self._create_entry_parameter_set(dataset, entry.id, entry.updated)
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
        logger.debug('Fetching feed: %s' % url)
        return feedparser.parse(url, handlers=[self.get_credential_handler()])


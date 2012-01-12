from celery.task import task
from django.conf import settings
from django.db import DatabaseError
from tardis.tardis_portal.staging import write_uploaded_file_to_dataset
from tardis.apps.atomimport.atom_ingest import AtomWalker
import urllib2, os


@task(name="atom_ingest.walk_feed", ignore_result=True)
def walk_feed(feed):
    try:
        AtomWalker(feed).ingest()
    except DatabaseError, exc:
        walk_feed.retry(args=[feed], exc=exc)


@task(name="atom_ingest.walk_feeds", ignore_result=True)
def walk_feeds(*feeds):
    for feed in feeds:
        walk_feed.delay(feed)


@task(name="atom_ingest.make_local_copy", ignore_result=True)
def make_local_copy(datafile):
    try:
        opener = urllib2.build_opener((AtomWalker.get_credential_handler()))
        f = opener.open(datafile.url)
        f_loc = write_uploaded_file_to_dataset(datafile.dataset, f, \
                                               datafile.filename)
        base_path = os.path.join(settings.FILE_STORE_PATH,
                          str(datafile.dataset.experiment.id),
                          str(datafile.dataset.id))
        datafile.url = 'tardis://' + os.path.relpath(f_loc, base_path)
        datafile.save()
    except DatabaseError, exc:
        make_local_copy.retry(args=[datafile], exc=exc)

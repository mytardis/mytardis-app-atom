from celery.task import task
from django.conf import settings
from django.db import DatabaseError
from tempfile import NamedTemporaryFile

from tardis.tardis_portal.staging import stage_file
from .atom_ingest import AtomWalker
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
        stage_file(datafile,
                   urllib2.build_opener((AtomWalker.get_credential_handler())))
    except DatabaseError, exc:
        make_local_copy.retry(args=[datafile], exc=exc)


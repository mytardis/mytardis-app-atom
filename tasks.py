from celery.task import task
from django.conf import settings
from django.db import DatabaseError
from tempfile import NamedTemporaryFile

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

MyTardis Atom App
=================

This app can be used to ingest datasets via Atom. Please see `tests/atom_test` for format examples.

New metadata is ingested first, with data files being copied asynchronously afterwards.

Installation
------------

Symlink this app into a MyTardis `tardis/apps` directory. The preferred name for the app is `atom`.

Configuration
-------------

Celery is used to schedule periodic file ingestion. Import from Picasa is supported to provide a
quick example, but in most deployments you'll want to write your own Atom producer.

The `atom_ingest.walk_feeds` task takes a variable number of feeds and updates them. Here's an example
for `settings.py` that checks two Picassa feeds every 30 seconds:

    CELERYBEAT_SCHEDULE = {
      "update-feeds": {
        "task": "atom_ingest.walk_feeds",
        "schedule": timedelta(seconds=30),
        "args": ('http://picasaweb.google.com/data/feed/base/all?prettyprint=true&tag=wombat&kind=photo',
                 'http://picasaweb.google.com/data/feed/base/all?prettyprint=true&tag=numbat&kind=photo')
      },
    }

You must run [celerybeat][celerybeat] and [celeryd][celeryd] for the scheduled updates to be performed.
MyTardis provides a `Procfile` for this purpose, but you can run both adhoc with:

    bin/django celeryd --beat

HTTP Basic password protection is available via `settings.py`:

    ATOM_FEED_CREDENTIALS = [
      ('http://localhost:4272/', 'username', 'password')
    ]

In a production environment, you should combine HTTP Basic password protection with SSL for security.


[celerybeat]: http://ask.github.com/celery/userguide/periodic-tasks.html#starting-celerybeat
[celeryd]: http://ask.github.com/celery/userguide/workers.html#starting-the-worker
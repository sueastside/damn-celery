from __future__ import absolute_import

from celery import Celery

app = Celery('damn_celery',
             broker='redis://',
             backend='redis://',
             include=['damn_celery.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERY_CHORD_PROPAGATES=True,
    CELERY_TRACK_STARTED=True,
)

if __name__ == '__main__':
    app.start()

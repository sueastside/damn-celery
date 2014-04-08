import multiprocessing, logging # Fixes error 'info('process shutting down') TypeError: 'NoneType' object is not callable'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Digital Assets Managed Neatly: Celery wrapper',
    'author': 'sueastside',
    'url': 'https://github.com/sueastside/damn-celery',
    'download_url': 'https://github.com/sueastside/damn-celery',
    'author_email': 'No, thanks',
    'version': '0.1',
    'test_suite': 'tests.suite',
    'install_requires': ['damn-at', 'celery[redis]'],
    'packages': ['damn_celery'],
    'scripts': [],
    'name': 'damn_celery',
    'entry_points':{
          'console_scripts':['damn-celery = damn_celery:main']
    }
}

setup(**config)

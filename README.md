DAMN-CELERY
====
Digital Assets Managed Neatly - Celery wrapper


http://sueastside.github.io/damn-celery/


Installation
----- 
Install Redis
 ```
sudo apt-get install redis-server
 ```
 
 Usage
 -----
 Run the worker.
 ```
 celery worker --app=damn_celery -l info --autoreload
 ```

from __future__ import absolute_import

import os
import hashlib
import urllib2
from time import sleep
import zipfile

from bs4 import BeautifulSoup

from celery import task
from celery import group, chain, subtask, chord, uuid
from celery.result import AsyncResult, GroupResult

from damn_celery.celery import app

import logging
logger = logging.getLogger('damn_celery.tasks')


def set_progress(task_id, current, total=100, status='PROGRESS', extra={}):
    """
    Set progress in the backend for the given task_id
    """
    meta = {
        'current': current,
        'total': total,
        'status': status,
    }
    meta.update(extra)
    app.backend.store_result(task_id, meta, status)
    #app.backend.mark_as_started(task_id, **meta)
    logger.info('setting progress: %s', str(meta))


def uuidfromurl(url):
    """
    Generate a unique identifier for the given url
    """
    m = hashlib.sha1(url)
    return m.hexdigest()
    

def has_completed(job_id):
    """
    Run whether the task and its subtasks have completed  
    """
    job = AsyncResult(job_id)
    return job.ready() and (not job.successful() or (hasattr(job.result, 'ready') and job.result.ready()))


def collect_stats(job_id):
    """
    Collect statistics about the given OGA job.
    """
    data = {}
    
    parse_url = AsyncResult(job_id+'_parse_url')
    data['state'] = parse_url.state
    data['jobs'] = []
    dmap = AsyncResult(job_id+'_dmap')
    info = dmap.info
    total = 0
    if info:
        for child in info.get('__subtasks', []):
            total += 1
            if type(child) != type({}):
                data['jobs'].append(child.info)
            else:
                if 'status' in child:
                    data['jobs'].append(child)
    
    if dmap.state != 'SUCCESS' and parse_url.state == 'SUCCESS':
        data['state'] = 'PROCESSING'
    
    data['current'] = total - len(data['jobs'])
    data['total'] = total
    
    return data, has_completed(job_id)


def collect_stats_transcoding(group_id):
    """
    Collect statistics about the given transcoding group job.
    """
    group = GroupResult.restore(group_id)
    data = {}
    data['state'] = 'TRANSCODING'
    data['current'] = group.completed_count()
    data['total'] = len(group)
    
    return data, group.ready()


class MemoryMetaDataStore(object):
    """
    A memory MetaDataStore implementation.
    """
    def __init__(self):
        self.descriptions = {}

    def is_in_store(self, store_id, an_hash):
        """
        Check if the given file hash is in the store.
        """
        return an_hash in self.descriptions
        
    def get_metadata(self, store_id, an_hash):
        """
        Get the FileDescription for the given hash.
        """
        return self.descriptions[an_hash]
           
    def write_metadata(self, store_id, an_hash, a_file_descr):
        """
        Write the FileDescription to this store.
        """
        self.descriptions[an_hash] = a_file_descr
        return a_file_descr 


#==[ Tasks ]============================================================    
    
@app.task()
def collect(group):
    logger.debug('%s', str(group))
    return group


@app.task(bind=True)
def dmap(self, it, callback):
    """
    Map a callback over an iterator and return as a group
    """
    callback = subtask(callback)
    tasks = group([callback.clone([arg,]) for arg in it])

    _tasks = tasks()
    ids = map(lambda x: x, _tasks)
    app.backend.mark_as_started(self.request.id, **{'__subtasks': ids})
    return _tasks
 

@app.task(bind=True)
def parse(self, file_path, url):
    """
    Open and parse the given file_path and extract contained file urls
    """
    logger.debug('Tasks::parse %s -> %s', str(file_path), str(self.request.id))

    urls = []
    
    with open(file_path, 'rb') as file:
        parsed_html = BeautifulSoup(file.read())
        if parsed_html:
            files = parsed_html.select('div.field-items span.file')
            for f in files:
                urls.append(f.a['href'])
                
    return urls
    

@app.task(bind=True)
def download(self, url):
    """
    Download the given url to a temporary file with uuidfromurl() as file name.
    """
    logger.debug('Tasks::download %s -> %s', str(url), str(self.request.id))
    
    path = os.path.join('/tmp/oga', uuidfromurl(url))
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    if not os.path.exists(path):
        with open(path, 'wb') as out:
            u = urllib2.urlopen(url)
            meta = u.info()
            header = meta.getheaders("Content-Length")
            if len(header):
                file_size = int(meta.getheaders("Content-Length")[0])
            else:
                file_size = -1

            set_progress(self.request.id, 0, file_size, 'DOWNLOADING', extra={'url': url})

            file_size_dl = 0
            block_sz = 8192
            while True:
                buffer = u.read(block_sz)
                if not buffer:
                    break

                file_size_dl += len(buffer)
                out.write(buffer)
                
                set_progress(self.request.id, file_size_dl, file_size, 'DOWNLOADING', extra={'url': url})
    else:
        logger.info('Tasks::download: %s exists for url %s', path, url)

    return path


@app.task(bind=True)
def analyze(self, file_path):
    """
    Possibly unzip and analyze with DAMN the given file(s) contained in file_path
    and return the analyzed metadata.
    """
    logger.debug('Tasks::analyze %s -> %s', str(file_path), str(self.request.id))
    
    files = []
    
    if not zipfile.is_zipfile(file_path):
        set_progress(self.request.id, 0, 1, 'ANALYZING', extra={'file_path': file_path})
        files.append(file_path)
    else:
        zfile = zipfile.ZipFile(file_path)
        set_progress(self.request.id, 0, len(zfile.namelist()), 'ANALYZING', extra={'file_path': file_path})
        for name in zfile.namelist():
          logger.debug('%s', name)
          (dirname, filename) = os.path.split(name)
          logger.debug('%s  %s', dirname, filename)
          dirname = os.path.join('/tmp/oga', dirname)
          logger.debug('Decompressing %s on %s', filename, dirname)
          if not os.path.exists(dirname):
            os.makedirs(dirname)
          zfile.extract(name, dirname)
          files.append(os.path.join(dirname, name))
        zfile.close()
        

    from thrift.protocol.TJSONProtocol import TSimpleJSONProtocol
    from damn_at import Analyzer, FileId, FileDescription
    from damn_at.analyzer import analyze, AnalyzerFileException, AnalyzerUnknownTypeException
    from damn_at.serialization import SerializeThriftMsg
    
    analyzer = Analyzer()
    metadatastore = MemoryMetaDataStore()
    
    metadata = {}    
    for i, filename in enumerate(files):
        try:
            descr = analyze(analyzer, metadatastore, filename, logging, True)
            #descr = SerializeThriftMsg(descr, TSimpleJSONProtocol)
        except (AnalyzerFileException, AnalyzerUnknownTypeException) as afe:
            logger.warning('EXCEPTION: %s --> %s', afe, filename)
            fileid = FileId(filename = filename, hash='NOT_FOUND')
            descr = FileDescription(file = fileid)
            descr.assets = []
                
        metadata[filename] = descr
        set_progress(self.request.id, i+1, len(files), 'ANALYZING', extra={'file_path': file_path})

    return metadata


@app.task(bind=True)
def transcode(self, file_descr, asset_id, dst_mimetype='image/png', options={}):
    """
    Transcode the given asset_id to the destination mimetype
    and return the paths to the transcoded files.
    """
    logger.info('transcode: %s -> %s', str(asset_id), dst_mimetype)   
    
    from damn_at import Transcoder
    path = '/tmp/transcoded/'
    t = Transcoder(path)    
    
    preview_paths = []
    
    
    target_mimetype = t.get_target_mimetype(asset_id.mimetype, dst_mimetype)
    if target_mimetype:
        for size in ['256,256']:
            options = t.parse_options(asset_id.mimetype, target_mimetype, size=size, angles='0')
            
            paths = t.get_paths(asset_id, target_mimetype, **options)    
            exists = all(map(lambda x: os.path.exists(os.path.join(path, x)), paths))
            print paths, exists
            if not exists:
                #print('Transcoding', asset_id.subname, file_descr.file.filename)
                paths = t.transcode(file_descr, asset_id, dst_mimetype, **options)
            #print('get_paths', asset_id.mimetype, paths)
            preview_paths.append((size.replace(',', 'x'), paths, ))
    else:
        #print(t.get_target_mimetypes().keys())
        print('get_paths FAILED', asset_id.mimetype, dst_mimetype)
         
    return preview_paths


@app.task(bind=True)
def parse_url(self, url):
    """
    Download and parse the given url.
    Contained in a separate task together so we can check on progress 
    given the same task_id. 
    """
    task_id = self.request.id
    
    #raise Exception('BLAH')
    
    file_path = download.s(url).set(task_id=task_id).apply().get()
    
    urls = parse.s(file_path, url).set(task_id=task_id).apply().get()
    
    set_progress(self.request.id, 0, len(urls), 'ANALYZING')
    return urls


@app.task(bind=True)
def process_url(self, url):
    """
    Download and analyze the given url.
    Contained in a separate task together so we can check on progress 
    given the same task_id.
    """
    task_id = self.request.id
    print('Tasks::process_url', url, task_id)
    #work = chain(download.s(url).set(task_id=task_id+'_download'), analyze.s().set(task_id=task_id+'_analyze'))
    #return work()
    
    file_path = download.s(url).set(task_id=task_id).apply().get()
    print('Tasks::process_url2', file_path)
    ana = analyze.s(file_path).set(task_id=task_id).apply().get()
    #print('Tasks::process_url3', ana)
    return ana


@app.task
def error_handler(uuid, task_id):
    print('Tasks::error_handler', uuid, task_id)
    from celery.result import AsyncResult
    result = AsyncResult(uuid)
    exc = result.get(propagate=False)
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(
          uuid, exc, result.traceback))
    app.backend.mark_as_failure(task_id, exc, traceback=result.traceback)

    
def oga(url, task_id=uuid()):
    """
    Start an async task to download all files on a given OpenGameArt url
    and analyze them returning the FileDescriptions for each file.
    """
    print('Tasks::oga', task_id)
    urls = chain(parse_url.s(url).set(task_id=task_id+'_parse_url', link_error=error_handler.s(task_id=task_id)), 
                 dmap.s(process_url.s()).set(task_id=task_id+'_dmap', link_error=error_handler.s(task_id=task_id)),
                 collect.s()
                 )
    return urls.apply_async(task_id=task_id)
    
    
def generate_previews_for_filedescriptions(file_descrs, task_id=uuid()):
    """
    Generate a webpreview for each asset contained in the given FileDescriptions.
    """
    callbacks = []
    for file_descr in file_descrs:
        for asset in file_descr.assets:
            callbacks.append(transcode.s(file_descr, asset.asset))
    paths = group(callbacks)
    async_group = paths.apply_async(task_id=task_id)
    async_group.save()
    return async_group

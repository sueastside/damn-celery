"""
Test tasks
"""
import os
import unittest
import uuid

from celery.result import AsyncResult, GroupResult

from damn_celery import tasks

#celery worker --app=damn_celery -l info --autoreload --purge

import logging
logger = logging.getLogger('damn_celery.tasks')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

class TestCase(unittest.TestCase):
    """Test case"""
    def test_uuidfromurl(self):
        """Test uuidfromurl()"""
        url = 'http://opengameart.org/content/fieldstone-fireplace'
        
        assert tasks.uuidfromurl(url) == 'd77b42a800ce66d2018e30ff181d508daf3f821b'


    def test_extensionfromurl(self):
        """Test uuidfromurl()"""
        url = 'http://opengameart.org/content/fieldstone-fireplace'

        assert tasks.extensionfromurl(url) == ''
        
        url = 'http://opengameart.org/sites/default/files/fireplace_fieldstone.blend.zip'

        assert tasks.extensionfromurl(url) == '.zip'
        
        url = 'http://opengameart.org/sites/default/files/van.blend'

        assert tasks.extensionfromurl(url) == '.blend'
        

    def test_download(self):
        """Test download()"""
        url = 'http://opengameart.org/content/fieldstone-fireplace'
        
        path = os.path.join('/tmp/oga', tasks.uuidfromurl(url))
        
        if os.path.exists(path):
            os.remove(path)
        
        result = tasks.download.s(url).apply()
        
        file_path = result.get()
        
        assert result.ready()
        
        assert result.successful()
        
        assert os.path.exists(file_path)
        
        
    def test_parse(self):
        """Test parse()"""
        url = 'http://opengameart.org/content/fieldstone-fireplace'
        
        result = tasks.download.s(url).apply()
        file_path = result.get()
        
        result = tasks.parse.s(file_path, url).apply()
        
        assert result.ready()
        
        assert result.successful()
        
        urls = result.get()
        
        assert urls == ['http://opengameart.org/sites/default/files/fireplace_fieldstone.blend.zip']


    def test_analyze(self):
        """Test parse()"""
        #url = 'http://opengameart.org/sites/default/files/fireplace_fieldstone.blend.zip'
        url = 'http://opengameart.org/sites/default/files/van.blend'
        
        result = tasks.download.s(url).apply()
        file_path = result.get()
        
        result = tasks.analyze.s(file_path).apply()
        
        assert result.ready()
        
        #assert result.successful()
        
        print result.get()


    def test_transcode(self):
        """Test parse()"""
        url = 'http://opengameart.org/sites/default/files/fireplace_fieldstone.blend.zip'
        
        result = tasks.download.s(url).apply()
        file_path = result.get()
        
        result = tasks.analyze.s(file_path).apply()
        
        analyzed = result.get()
        
        file_descr = analyzed.values()[0]
        
        results = tasks.group([tasks.transcode.s(file_descr, asset.asset) for asset in file_descr.assets]).apply()
        
        for paths in results:
            print 'DAMN', paths.result, getattr(paths, 'traceback', None)

        
    def test_oga(self):
        import redis
        rs1 = redis.Redis(host="localhost", db=0)
        rs1.flushdb()
        
        res = tasks.oga('http://opengameart.org/content/fieldstone-fireplace')
        job_id = res.id
        
        print 'STARTING', res.id

        
        while True:
            stats, ready = tasks.collect_stats(job_id)
            print stats, ready, tasks.has_completed(job_id)
            if ready:
                job = AsyncResult(job_id)
                print 'test_oga1:', job.info, job.state, job.ready(), job.successful(), job.result
                break

        print tasks.collect_stats(job_id)        
        if job.successful():
            print 'test_oga2:', [x.result for x in job.result]
        else:
            print 'test_oga3: FAILED'
            
        descriptions = []
        for result in job.result:
            descriptions.extend(result.get().values())
        group = tasks.generate_previews_for_filedescriptions(descriptions)
        group_id = group.id
        

        while True:
            stats, ready = tasks.collect_stats_transcoding(group_id)
            print stats, ready
            if ready:
                break

        assert True
    

def test_suite():
    """Return a list of tests"""
    return unittest.TestLoader().loadTestsFromTestCase(TestCase)

if __name__ == '__main__':
    #unittest.main()
    unittest.TextTestRunner().run(test_suite())

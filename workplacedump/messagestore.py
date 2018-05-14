import hashlib

from pymongo import MongoClient, ASCENDING, DESCENDING

from nsqstore import NsqStore
from deeputil import Dummy

DUMMY_LOG = Dummy()

class MessageStore(object):
    def __init__(self, db_name='workplacedb', collection_name='workplacedump', log=DUMMY_LOG):
        self.log = log
        self.client = MongoClient()
        self.db_name = db_name
        self.collection_name = collection_name
        self.nsqstore_obj = NsqStore(log=self.log)

    def insert_into_db(self, record):
        self.log.info('Inserting into db', msg=record)

        if 'field' in record and record['field'] == 'page' and 'message' in record:
            self.log.info('Inserting data')
            key = self.make_key(record['message']['mid'], record['timestamp'])
            self.log.debug('Checking for duplicate record')
            duplicate_record = self.client[self.db_name][self.collection_name].find({'key': key})
            if duplicate_record.count() == 0:
                self.log.info('No duplicates Found')
                record['key'] = key
                flag = self.nsqstore_obj.store_in_nsq(str(record))

                if flag:
                    self.log.info("Stored in nsq", msg=record)
                self.client[self.db_name][self.collection_name].insert(record)
                return True
        elif 'field' in record and 'permalink_url' in record:
            key = self.make_key(record['permalink_url'], record['created_time'])
            self.log.info('Checking for duplicate record')
            duplicate_record = self.client[self.db_name][self.collection_name].find({'key': key})

            if duplicate_record.count() == 0:
                self.log.info('No duplicates Found')
                record['key'] = key
                flag = self.nsqstore_obj.store_in_nsq(str(record))

                if flag:
                    self.log.info("Stored in nsq", msg=record)
                self.client[self.db_name][self.collection_name].insert(record)
                return True
            elif record['field'] == 'posts':
                for dup_record in duplicate_record:
                    if dup_record['time'] < record['time'] and 'message' in record:
                        self.log.info('updating updated time')
                        self.client[self.db_name][self.collection_name].update({'key' : key}, {'$set' : {'time' : record['time'], 'message' : record['message']}})
            elif record['field'] == 'comments':
                for dup_record in duplicate_record:
                    if 'message' in record and 'message' in dup_record and record['message'] != dup_record['message']:
                        flag = self.nsqstore_obj.store_in_nsq(str(record))
                        if flag:
                            self.log.info("Stored in nsq")
                        self.log.info('updating comment')
                        self.client[self.db_name][self.collection_name].update({'key' : key}, {'$set' : {'time' : record['time'], 'message' : record['message']}})
                return False

    def make_key(self, permalink_url, created_time):
        self.log.info('Making key for the record')
        return hashlib.sha1((str(permalink_url) + str(created_time)).encode('utf8')).hexdigest()

    def get_last_time(self, group_id):
        last_post = self.client[self.db_name][self.collection_name].find(
            {'group_id': group_id, 'field': 'posts'}).sort('time', ASCENDING).limit(1)
        for post in last_post:
            return post['time']

    def get_latest_time(self, group_id):
        first_post = self.client[self.db_name][self.collection_name].find(
            {'group_id': group_id, 'field': 'posts'}).sort('time', DESCENDING).limit(1)
        for post in first_post:
            return post['time']

    def get_comments_latest_ts(self, post_id):
        first_comment = self.client[self.db_name][self.collection_name].find(
            {'post_id': post_id, 'field': 'comments'}).sort('time', DESCENDING).limit(1)
        for comment in first_comment:
            return comment['time']

    def get_replies_latest_ts(self, comment_id):
        first_reply = self.client[self.db_name][self.collection_name].find(
            {'comment_id': comment_id, 'field': 'comments'}).sort('time', DESCENDING).limit(1)
        for reply in first_reply:
            return reply['time']

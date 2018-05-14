import time
import os

import facebook
from deeputil import Dummy, AttrDict

from messagestore import MessageStore

DUMMY_LOG = Dummy()


class WorkplaceHistory(object):
    def __init__(self, auth_token, log=DUMMY_LOG):
        self.auth_token = auth_token
        self.log = log

        self.graph = facebook.GraphAPI(self.auth_token)
        # FIXME: how does this work?
        os.environ['TZ'] = 'GMT'
        self.connect_ts = int(time.time())
        self.pattern = '%Y-%m-%dT%H:%M:%S+0000'
        self.msgstore = MessageStore(log=self.log)

    def get_groups(self):
        url = 'community?fields=groups.limit(1)'
        groups_info = self.graph.get_object(url)
        if 'groups' in groups_info:
            groups_info = groups_info['groups']

        while True:
            groups = groups_info['data']
            for group in groups:
                group = AttrDict(group)
                self.log.info("Fetching data for group :" + group.name)
                last_post_ts = self.msgstore.get_last_time(group.id)

                if not last_post_ts:
                    last_post_ts = self.connect_ts

                self.get_post_feed(group.id, group.name, until=last_post_ts)
                first_post_ts = self.msgstore.get_latest_time(group.id)

                if not first_post_ts:
                    first_post_ts = self.connect_ts

                if not first_post_ts == self.connect_ts:
                    self.get_post_feed(group.id, group.name, until=self.connect_ts, since=first_post_ts)

            if 'next' in groups_info['paging']:
                url = groups_info['paging']['next'].split('/', 4)[4]
                groups_info = self.graph.get_object(url)
            else:
                break

            self.log.debug('Fetching complete')

    def get_threads(self):
        url = 'community?fields=members{conversations}'
        members_info = self.graph.get_object(url)
        # Every member will have same coversations so we check for only one member threads.
        threads_info = members_info['members']['data'][0]['conversations']
        while True:
            threads = threads_info['data']
            for thread in threads:
                self.log.info('Fetching for thread')
                self.get_thread_convo(thread)
                self.log.info('Fetch complete')

            if 'next' in threads_info['paging']:
                url = threads_info['paging']['next'].split('/', 4)[4]
                threads_info = self.graph.get_object(url)
            else:
                break

    def get_thread_convo(self, thread):
        conversation = self.graph.get_object(thread['id'] + "?fields=messages.limit(100){message,created_time,id,from,to,tags,attachments{file_url,name,image_data,mime_type,size,video_data,id},shares.limit(10){name,link,id,description},sticker}")
        if 'messages' in conversation:
            conversation = conversation['messages']
        # import pdb; pdb.set_trace()
        while True:
            convo = conversation['data']
            for message in convo:
                temp = {}
                temp['text'] = message['message']
                temp['mid'] = message['id']
                message.pop('id')
                message.pop('message')
                message['message'] = temp
                message['sender'] = message.pop('from')
                message['recipient'] = message.pop('to')
                message['recipient'] = message['recipient']['data'][0]
                message['thread'] = thread
                message_ts = str(message['created_time'])
                message['timestamp'] = int(time.mktime(time.strptime(message_ts, self.pattern)))
                message['field'] = 'page'
                message['message']['mid'] = message['message']['mid'].split('_', 1)[1]
                flag = self.msgstore.insert_into_db(message)
                if flag:
                    self.log.info('message_stored', msg=message)
            if 'next' not in conversation['paging']:
                break
            conversation = self.graph.get_object(conversation['paging']['next'].split('/', 4)[4])

    def get_post_feed(self, group_id, group_name, until=int(time.time()), since=0):
        self.log.debug('Getting Posts')
        url = '%s ?fields=feed.limit(100).until(%s).since(%s){from,message,\
                story,permalink_url,created_time,updated_time,type,attachments\
                {subattachments,title,url,type,target}}'
            feed = self.graph.get_object(url) % (group_id, str(until), str(since))

        if 'feed' in feed:
            feed = feed['feed']

        while True:
            if 'paging' not in feed:
                break

            posts = feed['data']

            for counter in range(0, len(posts)):
                post_ts = str(posts[counter]['updated_time'])
                posts[counter]['time'] = int(time.mktime(
                    time.strptime(post_ts, self.pattern)))
                posts[counter]['field'] = 'posts'
                posts[counter]['group_id'] = group_id
                posts[counter]['group_name'] = group_name
                posts[counter]['post_id'] = posts[counter]['id']

                latest_ts = self.msgstore.get_comments_latest_ts(
                    posts[counter]['id'])

                if latest_ts:
                    self.get_comments(posts[counter]['id'], group_id, group_name, until=int(
                        time.time()), since=latest_ts-1)
                else:
                    self.get_comments(
                        posts[counter]['id'], group_id, group_name)

                self.msgstore.insert_into_db(posts[counter])

            feed = self.graph.get_object(
                feed['paging']['next'].split('/', 4)[4])

    # FIXME: calling time.time() in default arg looks bad. need an alternative.
    def get_comments(self, post_id, group_id, group_name, until=int(time.time()), since=1):
        self.log.info('Getting comments')
        # FIXME: scary line
        comments = self.graph.get_object(post_id + '?fields=comments.limit(100).since(' + str(since) + ').until(' + str(
            until) + '){created_time,permalink_url,message,from,comment_count,message_tags,id,attachment}')

        while True:
            if 'comments' in comments:
                comments = comments['comments']

            if 'data' in comments:
                comment = comments['data']
            else:
                break

            for counter in range(0, len(comment)):
                self.get_comment_replies(
                    comment[counter]['id'], post_id, group_id, group_name)
                comment_ts = str(comment[counter]['created_time'])
                comment[counter]['time'] = int(time.mktime(
                    time.strptime(comment_ts, self.pattern)))
                comment[counter]['field'] = 'comments'
                comment[counter]['post_id'] = post_id
                comment[counter]['group_id'] = group_id
                comment[counter]['group_name'] = group_name

                latest_ts = self.msgstore.get_replies_latest_ts(
                    comment[counter]['id'])

                # FIXME: scary
                if latest_ts:
                    self.get_comment_replies(
                        comment[counter]['id'], post_id, group_id, group_name, until=self.connect_time, since=latest_ts-1)
                else:
                    self.get_comment_replies(
                        comment[counter]['id'], post_id, group_id, group_name)

                self.msgstore.insert_into_db(comment[counter])

            if 'next' in comments['paging']:
                comments = self.graph.get_object(
                    comments['paging']['next'].split('/', 4)[4])
            else:
                break

    def get_comment_replies(self, comment_id, post_id, group_id, group_name, since=1, until=int(time.time())):
        self.log.info('Getting replies')
        # FIXME: scary
        replies = self.graph.get_object(comment_id + '?fields=comments.limit(100).since(' + str(
            since) + ').until(' + str(until) + '){permalink_url,id,created_time,from,attachment}')

        while True:
            if 'comments' in replies:
                replies = replies['comments']

            if 'data' in replies:
                reply = replies['data']
            else:
                break

            for counter in range(0, len(reply)):
                reply_ts = str(reply[counter]['created_time'])
                reply[counter]['time'] = int(time.mktime(
                    time.strptime(reply_ts, self.pattern)))
                reply[counter]['field'] = 'comments'
                reply[counter]['post_id'] = post_id
                reply[counter]['group_id'] = group_id
                reply[counter]['group_name'] = group_name
                reply[counter]['comment_id'] = comment_id

                self.msgstore.insert_into_db(reply[counter])

            if 'next' in replies['paging']:
                replies = self.graph.get_object(
                    replies['paging']['next'].split('/', 4)[4])
            else:
                break

    def start(self):
        self.get_groups()
        self.get_threads()

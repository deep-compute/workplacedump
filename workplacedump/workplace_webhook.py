import time
import json

import tornado.httpserver
import tornado.ioloop
import tornado.web

import request
from basescript import BaseScript

# FIXME: py3 compatibility
from messagestore import MessageStore
from workplace_history import WorkplaceHistory


class RequestHandler(tornado.web.RequestHandler):

    def get(self):
        mode = self.get_argument('hub.mode')
        challenge = self.get_argument('hub.challenge')

        if mode == 'subscribe' and challenge:
            verify_token = self.get_argument('hub.verify_token')
            if not verify_token == 'auth_token':
                self.write("Verification Token Wrong")
                log = self.application.log
                log.error('auth_failed', verify_token=verify_token)

            self.write(challenge)

    def post(self):
        # FIXME: this code looks scary. need to simplify
        data = json.loads(self.request.body)
        print (data)
        # FIXME: danger
        temp1 = {}
        final_data = {}
        temp2 = {}
        #import pdb; pdb.set_trace()

        if 'object' in data and data['object'] == 'page':
            data = data['entry'][0]
            if 'messaging' in data:
                for key, value in data.iteritems():
                    if key != 'messaging':
                        final_data[key] = value
                final_data = data['messaging'][0]
                final_data['field'] = 'page'
                final_data['timestamp'] = int(str(final_data['timestamp'])[:-3])

        elif 'object' in data and data['object'] == 'group':
            # FIXME: py3 compat
            data = data['entry'][0]
            for key, value in data.iteritems():
                if key != 'changes':
                    final_data[key] = value
                else:
                    temp1[key] = value

            temp1 = temp1['changes'][0]
            for key, value in temp1.iteritems():
                if key != 'value':
                    final_data[key] = value
                else:
                    temp2[key] = value

            temp2 = temp2['value']
            for key, value in temp2.iteritems():
                final_data[key] = value

        print final_data
        message_store = self.application.message_store
        log = self.application.log
        success = message_store.insert_into_db(final_data)
        if success:
            log.info('message_stored', msg=final_data)

class WorkplaceWebhookScript(BaseScript):
    DESC = 'Tornado server for Facebook workplace webhooks'
    RETRY_WAIT = 5  # seconds

    def __init__(self, *args, **kwargs):
        # FIXME: py3 compat
        super(WorkplaceWebhookScript, self).__init__(*args, **kwargs)

        self.message_store = MessageStore(log=self.log)

    def _get_history(self):
        """while True:
            try:
                wh = WorkplaceHistory(auth_token=self.args.auth_token, log=self.log)
                wh.start()
            except Exception: # FIXME: catch only expected exception
                self.log.exception('workplace_history_get_failed')
        """
        wh = WorkplaceHistory(auth_token=self.args.auth_token, log=self.log)
        wh.start()

    def _listen_realtime(self):
        self.log.info('started realtime fetching')
        app = tornado.web.Application(handlers=[(r"/", RequestHandler)])
        app.message_store = self.message_store
        app.log = self.log

        http_server = tornado.httpserver.HTTPServer(app)
        http_server.listen(self.args.port)
        self.log.debug('starting HTTP server')
        tornado.ioloop.IOLoop.instance().start()

    def run(self):
        while True:
            try:
                self._get_history()
                self.log.info('History is fetched')
                self._listen_realtime()
            except Exception as e:
                self.log.exception('Exception', msg=e)
                time.sleep(self.RETRY_WAIT)
                self.run()

    def define_args(self, parser):
        # FIXME: add help docs
        parser.add_argument('auth_token', metavar='auth-token', help='Give Workplace authentication token')
        parser.add_argument('-p', '--port', default=8880, type=int, help='Give the port number where HTTP server should run')


if __name__ == '__main__':
    WorkplaceWebhookScript().start()

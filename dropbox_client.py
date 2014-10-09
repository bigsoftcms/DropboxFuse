#!/usr/bin/python2
# -*- coding: utf-8 -*-

import dropbox
from dropbox_config import DropboxConfiguration


class DropboxClient(dropbox.client.DropboxClient):
    def __init__(self, config, app_key=None, app_secret=None, access_token=None):
        self.config = DropboxConfiguration(config)
        if not ('app_key' in self.config and 'app_secret' in self.config):
            if app_key is None or app_secret is None:
                raise Exception('app_secret AND app_secret needs to be valid')
            self.config['app_key'] = app_key
            self.config['app_secret'] = app_secret

        if not ('access_token' in self.config and 'user_id' in self.config):
            ret = DropboxClient.get_access_token(self.config['app_key'], self.config['app_secret'])
            access_token, user_id = ret
            self.config['access_token'] = access_token
            self.config['user_id'] = user_id

        self.config.commit()
        super(DropboxClient, self).__init__(self.config['access_token'])

    @staticmethod
    def get_access_token(app_key, app_secret):
        flow = dropbox.client.DropboxOAuth2FlowNoRedirect(app_key, app_secret)

        # Have the user sign in and authorize this token
        authorize_url = flow.start()
        print '1. Go to: ' + authorize_url
        print '2. Click "Allow" (you might have to log in first)'
        print '3. Copy the authorization code.'
        code = raw_input("Enter the authorization code here: ").strip()

        # This will fail if the user enters an invalid authorization code
        access_token, user_id = flow.finish(code)
        return access_token, user_id

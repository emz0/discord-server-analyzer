
# coding: utf-8
from db.client import PGClient
from collections import ChainMap
from datetime import datetime
import re
import emoji
import pandas as pd
import numpy as np
import json
import os
import settings
from paralleldots import set_api_key, get_api_key, sentiment, similarity, emotion


class DiscussionAnalyzer:

    def __init__(self):
        self.IGNORED_MEMBER_IDS = settings.IGNORED_MEMBER_IDS
        self.client = PGClient()
        self.MAX_PIECE_LEN = 3000
        self.init_regex_patterns()
        set_api_key(os.environ.get('PARALLELDOTS_API_KEY'))


    def init_regex_patterns(self):
        unicode_emote_list = map(lambda x: ''.join(x.split()), emoji.UNICODE_EMOJI.keys())
        self.unicode_emote_ptrn = re.compile('|'.join(re.escape(p) for p in unicode_emote_list))
        self.custom_emote_ptrn = re.compile('<:\w+:[0-9]+>')
        self.tag_ptrn = re.compile('<@!?[0-9]+>')
        self.url_ptrn = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

    def clean(self, t):
        t = str(t)

        emotes = re.findall(self.custom_emote_ptrn, t)
        for e in emotes:
            e_new = e.split(':')[1].lower()
            t = re.sub(e, '', t)
        t = self.unicode_emote_ptrn.sub('', t)
        t = re.sub(self.url_ptrn, '', t)
        t = re.sub(self.tag_ptrn, '', t)
        t = re.sub('\n', ' ', t)

        t = re.sub('"', '', t)
        t = re.sub('\s+', ' ', t)

        return t.strip()

    def get_messages(self, first_id, last_id, by_member=False):
        q_messages = """SELECT json_build_object(
                            'id',id,'posted_at',posted_at,
                            'content',content, 'member_id', member_id)
                        FROM messages
                        WHERE channel_id = 263420076919750676
                        AND member_id NOT IN %s
                        AND id >= %s
                        AND id <= %s
                        ORDER BY id"""

        values = (self.IGNORED_MEMBER_IDS, first_id, last_id)
        cursor = self.client.query(q_messages, values)

        if by_member:
            messages = {}
            message = cursor.fetchone()
            while message:
                message = message[0]
                member_id = str(message['member_id'])

                if member_id in messages:
                    messages[member_id].append(message)
                else:
                    messages[member_id] = [message]

                message = cursor.fetchone()

        else:
            messages = [m[0] for m in cursor.fetchall()]

        return messages

    def extract_body(self, messages):
        return ' '.join([m['content'] for m in messages])

    def split_by_len(self, text):
        max_len = self.MAX_PIECE_LEN
        current_len = 0
        text_piece = ''
        texts = []
        for w in text.split(' '):
            if (current_len + len(w)) <= max_len:
                text_piece += w + ' '
            else:
                texts.append(text_piece.strip())
                text_piece = w + ' '
            current_len = len(text_piece)
        texts.append(text_piece.strip())

        return texts

    def scale_to_one(self, feelings):
        keys = feelings.keys()
        values = np.array(list(feelings.values()))
        values = values / values.sum()

        return dict(zip(keys, values))

    def get_avg_feelings(self, feelings):
        len_feelings = len(feelings)
        avg_feelings = {}
        for f, vals in feelings.items():
            avg_feelings[f] = sum([v for v in vals]) / len_feelings

        return avg_feelings

    def get_feelings(self, texts):
        t_feelings = {
            'sentiment': {
                's_negative': [],
                's_neutral': [],
                's_positive': []
            },
            'emotion': {
                'e_angry': [],
                'e_excited': [],
                'e_happy': [],
                'e_indifferent': [],
                'e_sad': []
            }
        }

        for t_part in texts:
            sent = sentiment(t_part)
            emot = emotion(t_part)

            t_feelings['sentiment']['s_negative'].append(sent['probabilities']['negative'])
            t_feelings['sentiment']['s_neutral'].append(sent['probabilities']['neutral'])
            t_feelings['sentiment']['s_positive'].append(sent['probabilities']['positive'])

            t_feelings['emotion']['e_angry'].append(emot['probabilities']['angry'])
            t_feelings['emotion']['e_excited'].append(emot['probabilities']['excited'])
            t_feelings['emotion']['e_happy'].append(emot['probabilities']['happy'])
            t_feelings['emotion']['e_indifferent'].append(emot['probabilities']['indifferent'])
            t_feelings['emotion']['e_sad'].append(emot['probabilities']['sad'])

        t_feelings['sentiment'] = (
                self.scale_to_one(
                        self.get_avg_feelings(t_feelings['sentiment']))
                )
        t_feelings['emotion'] = (
                self.scale_to_one(
                    self.get_avg_feelings(t_feelings['emotion']))
                )

        result = [(*t_feelings['sentiment'].keys(), *t_feelings['emotion'].keys()),
                  (*t_feelings['sentiment'].values(), *t_feelings['emotion'].values())
                ]
        return result

    def _analyze_by_member(self, first_id, last_id):
        member_d = self.get_messages(first_id, last_id, by_member=True)
        feelings = {}
        for member_id, d in member_d.items():
            d = self.split_by_len(
                                  self.clean(
                                    self.extract_body(d)))
            feelings[member_id] = self.get_feelings(d)

        return feelings

    def _analyze(self, first_id, last_id):
        d = self.get_messages(first_id, last_id)
        d = self.split_by_len(
                self.clean(
                    self.extract_body(d)))
        print("len of d:"+str(len(d)))
        feelings = self.get_feelings(d)
        return feelings

    def analyze(self, first_id, last_id, by_member=False):
        if by_member:
            return self._analyze_by_member(first_id, last_id)
        else:
            return self._analyze(first_id, last_id)

# d_author_feelings = json.load(open('d_author_feelings.json', 'r'))
# discussion_feelings = json.load(open('discussion_feelings.json', 'r'))


# df_d_a_feelings.to_csv('discussions_author_feelings.csv', sep=',')
# df_d_feelings.to_csv('discussions_feelings.csv', sep=',')

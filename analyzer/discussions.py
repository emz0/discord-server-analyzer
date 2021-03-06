
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
from paralleldots import (emotion, get_api_key, sentiment,
                          set_api_key, similarity)


class DiscussionAnalyzer:

    def __init__(self, channel_id,
                 first_id, last_id, type):
        set_api_key(settings.PARALLELDOTS_API_KEY)
        self.channel_id = channel_id
        self.first_id = first_id
        self.last_id = last_id
        self.type = type

        self.IGNORED_MEMBER_IDS = settings.IGNORED_MEMBER_IDS
        self.db_client = PGClient()
        self.MAX_PIECE_LEN = 3000
        self.init_regex_patterns()

    def init_regex_patterns(self):
        """
        Compile regex patterns
        """
        unicode_emote_list = map(lambda x: ''.join(x.split()),
                                 emoji.UNICODE_EMOJI.keys())
        self.unicode_emote_ptrn = re.compile('|'.join(re.escape(p)
                                             for p in unicode_emote_list))
        self.custom_emote_ptrn = re.compile('<:\w+:[0-9]+>')
        self.tag_ptrn = re.compile('<@!?[0-9]+>')
        self.url_ptrn = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

    def analyze(self):
        """
        Return analysis results
        """
        if self.type == 'both':
            return [self.__analyze(), self.__analyze_by_member()]
        elif self.type == 'member':
            return [self.__analyze_by_member()]
        else:
            return [self.__analyze()]

    def __analyze(self):
        """
        Analyze discussion emotions and sentiments
        """
        d = self.get_messages()
        d = self.clean(self.extract_body(d))
        num_of_characters = len(d)
        d = self.split_by_len(d)

        feelings = self.get_feelings(d)
        feelings[0] = ('num_of_characters',) + feelings[0]
        feelings[1] = (num_of_characters,) + feelings[1]
        return feelings

    def __analyze_by_member(self):
        """
        Analyze messages of each member separately
        """
        member_d = self.get_messages(by_member=True)
        results = []
        for member_id, d in member_d.items():
            d = self.clean(self.extract_body(d))
            num_of_characters = len(d)

            if num_of_characters < 1:
                continue

            d = self.split_by_len(d)

            feelings = self.get_feelings(d)

            if not results:
                header = ('member_id',
                          'member_name',
                          'num_of_characters') + feelings[0]
                results.append(header)

            member_name = self.__member_name_by_id(member_id)
            row = (member_id, member_name, num_of_characters) + feelings[1]

            results.append(row)

        return results

    def get_messages(self, by_member=False):
        """
        Return messages from db. If by_member == true,
        return dict where key is a member and value is
        the list of his messages
        """
        q_messages = """SELECT json_build_object(
                            'id',id,'posted_at',posted_at,
                            'content',content, 'member_id', member_id)
                        FROM messages
                        WHERE channel_id = %s
                        AND member_id NOT IN %s
                        AND id >= %s
                        AND id <= %s
                        ORDER BY id"""

        values = (self.channel_id, self.IGNORED_MEMBER_IDS,
                  self.first_id, self.last_id)
        cursor = self.db_client.query(q_messages, values)

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
        """
        Return the body of message
        """
        return ' '.join([m['content'] for m in messages])

    def clean(self, t):
        """
         Clean text from url, tags, emotes, multiple spaces, new line symbols and double quotes
        """
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

    def split_by_len(self, text):
        """
        Split text to segments where max length of
        each segment is MAX_PIECE_LEN
        """
        max_len = self.MAX_PIECE_LEN
        current_len = 0
        text_piece = ''
        texts = []
        for w in text.split(' '):
            if (current_len + len(w)) < max_len:
                text_piece += w + ' '
            else:

                texts.append(text_piece.strip())
                text_piece = w + ' '
            current_len = len(text_piece)
        texts.append(text_piece.strip())

        return texts

    def get_feelings(self, texts):
        """
        Return average of feelings values
        """
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

        result = [(*t_feelings['sentiment'].keys(),
                   *t_feelings['emotion'].keys()),
                  (*t_feelings['sentiment'].values(),
                   *t_feelings['emotion'].values())
                  ]
        return result

    def get_avg_feelings(self, feelings):
        """
        Return average of feelings values
        """
        len_feelings = len(feelings)
        avg_feelings = {}
        for f, vals in feelings.items():
            avg_feelings[f] = sum([v for v in vals]) / len_feelings

        return avg_feelings

    def scale_to_one(self, feelings):
        """
        Scale values of feelings to
        make their sum equal to 1
        """
        keys = feelings.keys()
        values = np.array(list(feelings.values()))
        values = values / values.sum()

        return dict(zip(keys, values))

    def __member_name_by_id(self, member_id):
        """
        Return member name by his id
        """
        q = "SELECT name FROM members WHERE id = %s"
        cursor = self.db_client.query(q, (member_id,))
        name = cursor.fetchone()[0]

        return name

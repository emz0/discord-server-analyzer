import queries as qs

DB_CONNECTION = 'postgresql://postgres:123456@localhost/jh'

DISCORD_SERVER_ID = '263420076919750676'

DISCORD_CHANNELS = [
    {'name': 'general', 'id': '263420076919750676'},
    {'name': 'mapping', 'id': '365883171545546772'},
    {'name': 'pokemon', 'id': '391333532204531713'},
    {'name': 'esports', 'id': '391225682107039754'},
    {'name': 'nsfw', 'id': '391225585898356736'}
]

ACTIVE_STATS = [
    ('most_reactions', qs.MOST_REACTIONS),
    ('most_reacted', qs.MOST_REACTED),
    ('activity_trend', qs.ACTIVITY_TREND),
    ('active_hours', qs.ACTIVE_HOURS),
    ('active_days', qs.ACTIVE_DAYS),
    ('most_used_emotes', qs.MOST_USED_EMOTES)
]

IGNORED_MEMBER_IDS = (
    155149108183695360,
    349547918966915073,
    159985870458322944
)

ANALYZE_DISCUSSIONS = True
DISCUSSIONS = [
    {'name': 'd1',
     'channel_id': 263420076919750676,
     'first_msg_id': 372759366392348683,
     'last_msg_id': 372782278138396673,
    }
]

import queries as qs

DB_CONNECTION = ''

DISCORD_CHANNELS = [
    {'server_id': '', 'channel_ids': []}
]

ACTIVE_STATS = [
    qs.MOST_REACTIONS,
    qs.MOST_REACTED,
    qs.ACTIVITY_TREND,
    qs.ACTIVE_HOURS,
    qs.ACTIVE_DAYS,
    qs.MOST_USED_EMOTES
]

IGNORED_MEMBER_IDS = (
    155149108183695360,
    349547918966915073,
    159985870458322944
)

ANALYZE_DISCUSSIONS = True

#used queries

MOST_REACTIONS = """
            WITH members_count AS (
                SELECT unnest(members) AS member_id, count(*) r_count
                FROM reactions 
                GROUP BY member_id
                )    
            SELECT m.name, mc.r_count
            FROM members_count mc
            JOIN members m ON m.id = mc.member_id
            WHERE mc.member_id != 349547918966915073
            ORDER BY r_count DESC
            LIMIT 15
        """

MOST_REACTED = """
            WITH reaction_count AS (
                SELECT sum(cardinality(r.members)) AS num_of_reactions, m.member_id
                FROM reactions r 
                JOIN messages m ON m.id = r.message_id 
                GROUP BY m.member_id 
            ),
                message_count AS (
                SELECT member_id, count(*) as num_of_messages
                FROM messages
                GROUP BY member_id        
            )
            SELECT m.name, rc.num_of_reactions, mc.num_of_messages, rc.num_of_reactions / mc.num_of_messages::float as ratio
            FROM reaction_count rc
            JOIN message_count mc ON mc.member_id = rc.member_id
            JOIN members m ON m.id = rc.member_id
            WHERE rc.member_id NOT IN (349547918966915073, 155149108183695360, 159985870458322944) 
            AND mc.num_of_messages >= 10
            ORDER BY ratio DESC
"""

ACTIVITY_TREND = """
            SELECT extract(month from posted_at)::int AS m, 
                   extract(year from posted_at)::int AS y, 
                   count(*)
            FROM messages
            GROUP BY y, m
            ORDER BY y,m
"""

ACTIVE_HOURS = """
            SELECT extract(hour from posted_at)::int AS hour_group, count(*)
            FROM messages GROUP BY hour_group
            ORDER BY hour_group
"""

ACTIVE_DAYS = """
            SELECT extract(isodow from posted_at)::int AS day_group, count(*)
            FROM messages GROUP BY day_group
            ORDER BY day_group
"""

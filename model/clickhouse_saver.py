"""
Small class working with clickhouse database
"""
from clickhouse_driver import Client
import os


class ClickHouseSaver:

    def __init__(self):
        """
        Creating connection to clickhouse database
        """
        password = os.environ.get('CLICKHOUSE_PASSWORD')
        db = os.environ.get('CLICKHOUSE_DATABASE')
        port = os.environ.get('CLICKHOUSE_PORT')
        self.client = Client('localhost', password=password,
                             database=db, port=port)

    def add_likes(self, likes):
        """
        Function creating and executing query inserting likes to database
        """
        fields = (("(UserId, ChannelId, Reaction, MessageId,")
                  + (" EventTime, EventDate, LikeTime, Url)"))
        query = f"INSERT INTO likes{fields} VALUES {str(likes)}"
        self.client.execute(query)

    def get_likes(self, tm):
        """
        Function getting likes from database added after last check
        tm describes the max like time which was received on previous iteration
        """
        query = f"""
        SELECT
            unique_messages.cid as Channel, unique_messages.mid as Message,
            unique_messages.url as Url,
            all_reactions.likes as Likes, all_reactions.dislikes as Dislikes,
            unique_messages.lt as Placed
        FROM
          (
            SELECT
                seq1.cid as cid1, seq1.mid as mid1,
                seq2.cid as cid2, seq2.mid as mid2,
                seq1.uuid as likes, seq2.uuid as dislikes
            FROM
            (
              SELECT
                cid, mid, COUNT(uid) as uuid, max(rc)
              FROM
                (
                  SELECT
                    cid, mid, uid, any(reac) as rc
                  FROM
                  (
                    SELECT
                        DISTINCT(ChannelId, MessageId, UserId) as msg,
                        ChannelId as cid, MessageId as mid, UserId as uid,
                        Reaction as reac
                    FROM likes
                    ORDER BY (EventTime, MessageId) DESC
                  )
                  GROUP BY (cid, mid, uid)
                )
              WHERE rc = 'like' GROUP BY (cid, mid)
            ) as seq1
            FULL OUTER JOIN
            (
              SELECT
                cid, mid, COUNT(uid) as uuid, max(rc)
              FROM
                (
                  SELECT
                    cid, mid, uid, any(reac) as rc
                  FROM
                  (
                    SELECT
                        DISTINCT(ChannelId, MessageId, UserId) as msg,
                        ChannelId as cid, MessageId as mid, UserId as uid,
                        Reaction as reac
                    FROM likes
                    ORDER BY (EventTime, MessageId) DESC
                  )
                  GROUP BY (cid, mid, uid)
                )
              WHERE rc = 'dislike' GROUP BY (cid, mid)
            ) as seq2
            ON (seq1.cid = seq2.cid AND seq1.mid = seq2.mid)
          ) as all_reactions
        JOIN
          (
            SELECT
                DISTINCT (ChannelId, MessageId, Url),
                MessageId AS mid, ChannelId AS cid, Url as url,
                max(EventTime) AS et, max(LikeTime) as lt
            FROM
                likes WHERE LikeTime > {tm}
            GROUP BY (ChannelId, MessageId, Url)
          ) as unique_messages
        ON (
                COALESCE(
                    nullIf(all_reactions.mid1, 0),
                    nullIf(all_reactions.mid2, 0)
                    ) = unique_messages.mid
                AND
                COALESCE(
                    nullIf(all_reactions.cid1, ''),
                    nullIf(all_reactions.cid2, '')
                ) = unique_messages.cid
            )
        ORDER BY Placed DESC"""
        reactions = self.client.execute(query)
        return reactions

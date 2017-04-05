"""
"it's some very good code"
https://cdn.discordapp.com/attachments/209002315145805824/299314385883627521/socks.png
"""

import sys
import os
import datetime

from io import StringIO
from parsers import get_parser
from model import engine, Placement

COMMIT_BATCH = 100000
DELETE_BATCH = 1000

if "postgres" in os.environ["DB_URL"]:
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute("SET DateStyle TO ISO")

def merge():
    parser = get_parser(sys.argv[1])
    commited = 0

    for lines in zip(*(iter(parser),) * COMMIT_BATCH):
        if "postgres" in os.environ["DB_URL"]:
            dump = StringIO()
            for line in lines:
                dump.write("\t".join([str(x) for x in line.values()]) + "\n")
            dump.seek(0)
            cursor.copy_from(dump, 'placements', columns=lines[0].keys())
            conn.commit()
        else:
            # http://docs.sqlalchemy.org/en/latest/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
            engine.execute(
                Placement.__table__.insert(),
                [line for line in lines]
            )

        commited = commited + len(lines)

        print("{commited} commited; {remaining} remaining".format(
            commited=commited,
            remaining=parser.total - commited
        ))

def filter_dupes():
    
    last_pixel = {}
    deletes = []

    for row in engine.execute(
        Placement.__table__.select().order_by(
            Placement.recieved_on
        )
    ):
        if row.author not in last_pixel:
            #sys.stdout.write("N")
            last_pixel[row.author] = {
                "time": row.recieved_on + datetime.timedelta(minutes=3),
                "x": row.x,
                "y": row.y,
                "color": row.color
            }
            continue

        if row.recieved_on < last_pixel[row.author]["time"] and (
            row.color != last_pixel[row.author]["color"] and
            row.x != last_pixel[row.author]["x"] and
            row.y != last_pixel[row.author]["y"]
        ):
            if "FUCKME" in os.environ:
                print(f"{row.author} dupe ({row.x}, {row.y}); color {row.color}")
                print("time left", last_pixel[row.author]["time"] - row.recieved_on)
                print("-"*60)
            deletes.append(row.id)
        else:
            last_pixel[row.author] = {
                "time": row.recieved_on + datetime.timedelta(minutes=3),
                "x": row.x,
                "y": row.y,
                "color": row.color
            }

    print(f"Deleting {len(deletes)} rows")
    deleted = 0

    for delete_ids in zip(*(iter(deletes),) * DELETE_BATCH):
        engine.execute(
            Placement.__table__.delete(
                Placement.id.in_(delete_ids)
            )
        )
        deleted = deleted + len(delete_ids)
        print(f"{deleted} deleted; {len(deletes) - deleted} remaining")

if __name__ == "__main__":
    if not "NOMERGE" in os.environ:
        merge()

    filter_dupes()

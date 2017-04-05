import os
import csv
import sqlite3
import time
import datetime


class Parser(object):
    def __init__(self, filename):
        if not os.path.exists(filename):
            raise Exception(filename + " does not exist.")

        self.filename = filename
        self._total = -1

    def get_length(self):
        return 0

    @property
    def total(self):
        if self._total == -1:
            self._total = self.get_length()

        return self._total

class CsvParser(Parser):
    BAD_CSV_MSG = "Csv file is an unknown type that should be implemented. (%s)"

    def __init__(self, filename):
        super().__init__(filename)

        with open(self.filename, "r") as f:
            reader = csv.reader(f)

            row = next(reader)

            # Expect six rows
            if len(row) != 6:
                raise Exception(self.BAD_CSV_MSG % "len {row} > 6".format(row=len(row)))
            
            # wgoodall check
            try:
                if row[1] == "timestamp":
                    row = next(reader)
    
                datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
                self.parser = self.parser_wgoodall
                return
            except ValueError:
                pass

            # Columns 0, 1, 2, 4, 5 should be integers.
            for x in [0, 1, 2, 4, 5]:
                try:
                    int(row[x])
                except (ValueError, TypeError):
                    raise Exception(self.BAD_CSV_MSG % "col {x}".format(x=x))
            
            # Assuming time is in col 5
            # Also assuming milliseconds is larger than normal time.
            if int(row[5]) > int(time.time()):
                # If it's the year 49217, please stop using this damn code.
                self.parser = lambda: self.parser_normal(milliseconds=True)
            else:
                self.parser = self.parser_normal

    def get_length(self):
        with open(self.filename, "r") as f:
            return sum(1 for line in f)

    def parser_wgoodall(self):
        time_parser = lambda t: datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")

        with open(self.filename, "r") as f:
            reader = csv.reader(f)

            # Skip the info header
            next(reader)

            for row in reader:
                # Skip missing timestamps.
                if not row[1]:
                    continue

                yield {
                    "x": int(row[3]),
                    "y": int(row[2]),
                    "author": row[5],
                    "color": int(row[4]),
                    "recieved_on": time_parser(row[1])
                }

    def parser_normal(self, milliseconds=False):
        # If it's the year 49217, please stop using this damn code.
        if milliseconds:
            time_parser = lambda t: datetime.datetime.utcfromtimestamp(int(t) / 1000.0)
        else:
            time_parser = datetime.datetime.utcfromtimestamp

        with open(self.filename, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                yield {
                    "x": int(row[2]),
                    "y": int(row[1]),
                    "author": row[3],
                    "color": int(row[4]),
                    "recieved_on": time_parser(row[5])
                }
    
    def __iter__(self):
        for row in self.parser():
            yield row


class SqliteParser(Parser):
    def get_length(self):
        with sqlite3.connect(self.filename) as conn:
            return conn.execute("SELECT COUNT(*) from placements").fetchone()[0]

    def __iter__(self):
        with sqlite3.connect(self.filename) as conn:
            for row in conn.execute("SELECT x, y, author, color, recieved_on from placements ORDER BY recieved_on"):
                yield {
                    "x": int(row[0]),
                    "y": int(row[1]),
                    "author": row[2],
                    "color": int(row[3]),
                    "recieved_on": datetime.datetime.utcfromtimestamp(row[4])
                }

def get_parser(uri: str):
    # lol blind
    if uri.endswith("csv"):
        parser = CsvParser(uri)
    elif uri.endswith(("sqlite", "sqlite3", "db")):
        parser = SqliteParser(uri)

    return parser
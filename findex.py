#!/usr/bin/python
# -*- Mode: python; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-

################################################################################
#
# Usage: findex.py command database [path]
# Commands:
# index - Index files under path, store hash values into the database
# duplicates (dups) - find duplicate file entries in the database
# query - ...
#
# 2DO:
# - [ ] Implement searching
# - [ ] double-pass indexing
#       First pass: count the number of files, update entries
#       for files that didn't change from the last indexing
#       Second pass:
#       Calculate hashes for new and changed files, insert new entries.
#       Should I add indexing progress bar?
# - [ ] Print indexing stats in the end
#
# Kirill Gorelov <kgorelov@gmail.com>
################################################################################


import os
import os.path
import optparse
import time
import sys
import hashlib
import sqlite3

################################################################################

class FileEntry:
    """
    This class represents file stat info combined with its sha1 hash value
    """
    def __init__(self, filename):
        pathname = os.path.realpath(
            os.path.expanduser(filename))
        stats = os.stat(pathname)
        self.filename = pathname
        self.sha1 = None
        self.size = stats.st_size
        self.ctime = stats.st_ctime

    def hash(self):
        f = open(self.filename, 'r')
        h = hashlib.sha1()
        h.update(f.read())
        f.close()
        self.sha1 = h.hexdigest()

################################################################################

class FIndexDB:
    """
    This class operates file index database
    """
    def __init__(self, dbfilename):
        self.dbfile = os.path.realpath(os.path.expanduser(dbfilename))
        # Connect to the database
        self.conn = sqlite3.connect(self.dbfile)
        cur = self.conn.cursor()
        # Create db if necessary
        self.create_db(cur)

    def create_db(self, cursor = None):
        cur = cursor if cursor is not None else self.conn.cursor()
        # Create tables when needed
        cur.execute("create table if not exists files (sha1 TEXT, name TEXT, size INTEGER, ctime INTEGER, generation INTEGER)")

    def lock_exclusive(self):
        self.conn.isolation_level = "EXCLUSIVE"

    def touch(self, fentry, generation):
        cur = self.conn.cursor()
        cur.execute("update files set generation=? where name=? and size=? and ctime=?",
                    (generation,
                     fentry.filename.decode('utf-8'),
                     fentry.size,
                     fentry.ctime))
        if cur.rowcount:
            print "U %s" % fentry.filename
            return True

        return False

    def store(self, fentry, generation):
        if fentry.sha1 is None:
            raise Exception("File entry is not hashed")
        cur = self.conn.cursor()
        cur.execute("insert into files values (?, ?, ?, ?, ?)",
                    (fentry.sha1,
                     fentry.filename.decode('utf-8'),
                     fentry.size,
                     fentry.ctime,
                     generation))
        print "A %s" % fentry.filename

    def lookup(self, fentry):
        # XXX 2DO: Impplement ME
        pass

    def drop_indexes(self):
        cur = self.conn.cursor()
        cur.execute("drop index if exists sha1idx")
        cur.execute("drop index if exists fileidx")

    def purge(self, generation):
        cur = self.conn.cursor()
        cur.execute("select name from files where generation <> ?", (generation,))
        for f in cur.fetchall():
            print "D %s" % f;
        cur.execute("delete from files where generation <> ?", (generation,))

    def commit(self):
        cur = self.conn.cursor()
        cur.execute("create index if not exists sha1idx on files (sha1)")
        cur.execute("create index if not exists fileidx on files (name, size, ctime)")
        self.conn.commit()

    def print_duplicates(self):
        cur = self.conn.cursor()
        #cur.execute('select * from files where sha1 in (select sha1 from files group by sha1 having (count(sha1) > 1))')
        cur.execute('select sha1, name from files where sha1 in (select sha1 from files group by sha1 having (count(sha1) > 1))')
        sha1 = None
        for r in cur.fetchall():
            if r[0] != sha1:
                if sha1 is not None:
                    print
                sha1=r[0]
            print "%s %s" % (r[0], r[1])

################################################################################

class FIndexer:
    def __init__(self, database, directory, generation):
        self.idxdb = database
        self.idxdir = directory
        self.generation = generation

    def index(self):
        self.idxdb.lock_exclusive()
        self.idxdb.drop_indexes()
        os.path.walk(self.idxdir, self.process_directory, None)
        self.idxdb.purge(self.generation)
        self.idxdb.commit()

    def process_directory ( self, args, dirname, filenames ):
        #print 'Directory',dirname
        for filename in filenames:
            p = os.path.join(dirname, filename)
            if not os.path.isfile(p):
                continue
            fe = FileEntry(p)
            if not self.idxdb.touch(fe, self.generation):
                fe.hash()
                self.idxdb.store(fe, self.generation)

################################################################################

class Main:
    def __init__(self):
        usage = "usage: %prog command database [path]"
        parser = optparse.OptionParser(usage=usage)

        (self.options, self.arguments) = parser.parse_args()

        if len(self.arguments) < 2:
            parser.print_help();
            sys.exit(1);

        # Start url
        self.cmd = self.arguments[0]
        self.dbfile = self.arguments[1]
        self.pathname = self.arguments[2] if len(self.arguments) >= 3 else None 

        # Set unbuffered stdout (XXX FIXME: breaks utf8, need to use condecs instead)
        # sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

    def run(self):
        if self.cmd == 'index':
            self.index()
        elif self.cmd == 'duplicates' or self.cmd == 'dups':
            self.printdups()
        else:
            print "Unknown command"
            return 1

    def index(self):
        database = FIndexDB(self.dbfile)
        indexer = FIndexer(database, self.pathname, int(time.time()))
        indexer.index()

    def printdups(self):
        database = FIndexDB(self.dbfile)
        database.print_duplicates()

if __name__ == "__main__":
    main = Main()
    sys.exit(main.run())

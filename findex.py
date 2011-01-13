#!/usr/bin/python
# -*- Mode: python; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-

################################################################################
#
# Usage: findex.py command database [direcory | file]
# Commands:
# index - Update database with files from directory
# query - ...
#
# 2DO: Implement searching, indexing progress bar, add logging support
# command line options and stuff
#
# Kirill Gorelov <kgorelov@gmail.com>
################################################################################


import os
import os.path
import time
import sys
import hashlib
import sqlite3


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
        cur.execute("create table if not exists files (sha1 TEXT primary key, name TEXT, size INTEGER, ctime INTEGER, generation INTEGER)")

    def lock_exclusive(self):
        self.conn.isolation_level = "EXCLUSIVE"

    def store(self, fentry, generation):
        if fentry.sha1 is None:
            raise Exception("File entry is not hashed")
        cur = self.conn.cursor()
        cur.execute("replace into files values (?, ?, ?, ?, ?)",
                    (fentry.sha1,
                     fentry.filename.decode('utf-8'),
                     fentry.size,
                     fentry.ctime,
                     generation))

    def has_entry_unrel(self, fentry):
        pass

    def lookup(self, fentry):
        # XXX 2DO: Impplement ME
        pass

    def drop_indexes(self):
        cur = self.conn.cursor()
        cur.execute("drop index if exists sha1idx")
        cur.execute("drop index if exists fileidx")

    def commit(self, generation):
        cur = self.conn.cursor()
        cur.execute("delete from files where generation <> ?", (generation,))
        cur.execute("create index if not exists sha1idx on files (sha1)")
        cur.execute("create index if not exists fileidx on files (name, size, ctime)")
        self.conn.commit()


class FIndexer:
    def __init__(self, database, directory, generation):
        self.idxdb = database
        self.idxdir = directory
        self.generation = generation

    def index(self):
        self.idxdb.lock_exclusive()
        self.idxdb.drop_indexes()
        os.path.walk(self.idxdir, self.process_directory, None)
        self.idxdb.commit(self.generation)

    def process_directory ( self, args, dirname, filenames ):
        #print 'Directory',dirname
        for filename in filenames:
            p = os.path.join(dirname, filename)
            if not os.path.isfile(p):
                continue
            print "Indexing: %s" % p
            fe = FileEntry(p)
            fe.hash()
            self.idxdb.store(fe, self.generation)
            #print 'File: %s [%s]' % (filename, hash_file(p))


################################################################################


database = FIndexDB("~/tmp/findexdb")
indexer = FIndexer(database, "/home/kgorelov/tmp", int(time.time()))
indexer.index()


#if __name__ == "__main__":
#    main = Main()
#    sys.exit(main.run())

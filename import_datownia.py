import sqlite3, sys, pymongo, os

SKIP_EXISTING = True

dnames = ["2010electionresults/data", "mpidlinktable/data", "voterecord/divisions", "voterecord/housebusiness", "voterecord/sessioninfo", "voterecord/vdm", "voterecord/votetype"]
mconn = pymongo.Connection()
db = mconn.rsparly2012

for dname in dnames:
    sdname = dname.replace("/", "")
    coll = db[sdname]
    coll.drop()

    conn = sqlite3.connect("%s.sqlite" % sdname)
    conn.row_factory = sqlite3.Row

    cur = conn.cursor()
    data = cur.execute("SELECT * FROM 'parlyhack2012/%s_1.0'" % dname)
    for i, r in enumerate(data):
        if i % 100 == 0: print i
        coll.insert(dict(r))
    # coll.insert(map(dict, data))

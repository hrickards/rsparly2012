import pymongo, sys
mconn = pymongo.Connection()
db = mconn.rsparly2012

db.ms2010.ensure_index([('towhy', 1)])
db.ms.ensure_index([('constituency', 1), ('todate', 1)])
db.mpidlinktabledata.ensure_index([('TWFY_Member_id', 1)])
db['2010electionresults'].ensure_index([('Parliament_Constituency_id', 1), ('AlphaSurname', 1)])
db.biodata.ensure_index([('dods_id', 1)])
db.voterecordvdm.ensure_index([('Parliament_People_id', 1), ('PARTY_NAME', 1), ('DivisionID', 1), ('VoteTypeID', 1)])
db.voterecordvotetype.ensure_index([('VoteTypeID', 1), ('VoteType', 1)])
db.voterecorddivisions.ensure_index([('DivisionID', 1)])
db.constituencieslatlon.ensure_index([('Parliament_Constituency_id', 1)])

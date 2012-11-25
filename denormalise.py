import pymongo, sys
from fish import ProgressFish

mconn = pymongo.Connection()
db = mconn.rsparly2012
ms2010 = db.ms2010
ms = db.ms
linkage = db.mpidlinktabledata
election_results = db['2010electionresults']
biodata = db.biodata
voterecordvdm = db.voterecordvdm
vote_types = db.voterecordvotetype
divisions = db.voterecorddivisions
coll = db.data
coll.drop()

LIMIT = int(1e4)

fish = ProgressFish(total=ms2010.count())
data = ms2010.find({"towhy": "still_in_office"}).limit(LIMIT)
for i, datum in enumerate(data):
    fish.animate(amount=i)
    mp = {}
    
    mp['first_name'] = datum['firstname']
    mp['last_name'] = datum['lastname']
    mp['party'] = datum['party']

    prev_datum = ms.find_one({'constituency': datum['constituency'], 'todate': '2010-04-12'})
    mp_change = prev_datum == None or prev_datum['firstname'] != datum['firstname'] or prev_datum['lastname'] != datum['lastname']
    mp['mp_change'] = mp_change
    mp['party_change'] = prev_datum == None or (not mp_change and prev_datum['party'] != datum['party'])

    mp['election_reason'] = datum['fromwhy'].replace("_", " ").capitalize()

    links = linkage.find_one({'TWFY_Member_id': datum["id"].split("/")[-1]})
    if links == None: continue
    candidate = election_results.find_one({'Parliament_Constituency_id': links['Parliament_Constituency_id'], 'AlphaSurname': datum['lastname']})
    if candidate == None: continue
    mp['region'] = candidate['Region']

    bio = biodata.find_one({'dods_id': links['DODS_id']})
    gender = bio['gender']
    mp['gender'] = gender

    votes = voterecordvdm.find({'Parliament_People_id':links['Parliament_People_id']})
    for vote in votes:
        division_id = vote['DivisionID']
        vote_type_id = vote['VoteTypeID']
        #vote_type = vote_types.find_one({'VoteTypeID': vote_type_id})['VoteType']
        #division = divisions.find_one({'DivisionID': division_id})['DivTitle']
        mp[division_id] = vote_type_id

    coll.insert(mp)

# Name - MS
# Party - MS
# Gender - bio
# Region - 2010
# MP change - MS
# Party change - MS
# Votes on bills - votes data
# Elected because of (general election)/etc - MS

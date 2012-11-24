import cherrypy, pymongo, json, os
import numpy as np

mconn = pymongo.Connection()
db = mconn.rsparly2012
coll = db.data
divisions = db.voterecorddivisions
vdm = db.voterecordvdm
voterecordvotetype = db.voterecordvotetype

EPSILON = 5

def possible_answers(key): return coll.distinct(key)
def question_text(key):
    labels = {'party': 'What political party are they?',
              'mp_change': 'Have they only been an MP since the last election?',
              'party_change': 'Has their party only been in power since the last election?',
              'election_reason': 'Why were they elected?',
              'region': 'What region is their constituency in?',
              'vvt': 'Estimate the voter turnout when they were last elected',
              'gender': 'What gender are they?'}
    if key in labels: return labels[key]
    else:
        div_title = divisions.find_one({'DivisionID': key})['DivTitle']
        return "How did they vote for %s (%d Labour MPs and %d Conservative MPs voted for this)?" % (div_title.split("-")[0].strip(","), labour_votes(key), tory_votes(key))
        raise "foo"

def answer_texts(key):
    # TODO mp_change, party_change, election_reason, vvt
    if key in ['party', 'region', 'gender']: answers = possible_answers(key)
    elif key in ['mp_change', 'party_change', 'election_reason', 'vvt']: answers = ["YOU PIECE OF SHIT HARRY IMPLEMENT IT"]
    else: answers = map(get_vote_type, possible_answers(key))
    answers.append("Not sure")
    return answers

def parse_answer(key, answer):
    if key in ['party', 'region', 'gender']: return answer
    elif key in ['mp_change', 'party_change', 'election_reason', 'vvt']: return ""
    else: return get_vote_id(answer)

def get_matching_mps(answers): return list(coll.find(dict(answers)))
def get_cost_answers(mps): return len(mps)

def tory_votes(key): return get_votes(key, 'Conservative')
def labour_votes(key): return get_votes(key, 'Labour')

def get_votes(key, party): return vdm.find({"PARTY_NAME": party, "DivisionID": key, "VoteTypeID": '1'}).count()

def get_vote_type(vote_type): return voterecordvotetype.find_one({'VoteTypeID':vote_type})['VoteType']
def get_vote_id(vote_type): return voterecordvotetype.find_one({'VoteType':vote_type})['VoteTypeID']

def least_covariance(orig_data, cols_to_skip, keys_to_skip):
    modelen = np.bincount(map(len, orig_data)).argmax()
    orig_data = filter(lambda r: len(r) == modelen, orig_data)

    cols_to_skip.extend(['first_name', 'last_name', '_id'])
    cols_to_hash = ['party', 'mp_change', 'party_change', 'election_reason', 'region', 'vvt', 'gender']

    data = map(lambda r: filter(lambda (k, v): k not in cols_to_skip, r.items()), orig_data)
    cols = [[k for k, v in r] for r in data]
    cols = list(set([element for lis in cols for element in lis]))

    hash_maps = {}
    for col in cols:
        if not col in cols_to_hash: continue
        vals = list(set(map(lambda r: r[col], orig_data)))
        mapping = dict(map(lambda r: r[::-1], enumerate(vals)))
        hash_maps[col] = mapping

    data = map(lambda r: map(lambda (k, v): int(hash_maps[k][v]) if k in cols_to_hash else int(v), r), data)
    data = np.transpose(np.array(data))

    covs = np.cov(data)
    covs = np.sum(covs**2, axis=1)
    covs *= 1/covs.max()
    covs = list(covs)

    covs = zip(cols, covs)
    covs = sorted(covs, key = lambda x: x[1], reverse=True)
    covs = filter(lambda r: not r[0] in keys_to_skip, covs)
    return covs[0][0]

def get_response_for_key(key): return {'question': question_text(key), 'question_id': key, 'answers': answer_texts(key)}

class App(object):
    def index(self, **answers):
        keys_to_skip = []
        for k in answers.keys():
            if answers[k] == "Not sure":
                keys_to_skip.append(k)
                del answers[k]
            else: answers[k] = parse_answer(k, answers[k])
        response = {}
        if len(answers) == 0:
            response = get_response_for_key('region')
        else:
            mps = get_matching_mps(answers)
            cost = get_cost_answers(mps)
            # TODO Don't just get first here; do proper cost function
            if cost == 0: response = {"error": "No match found"}
            elif cost < EPSILON: response = {"success": "%s %s" % (mps[0]['first_name'], mps[0]['last_name'])}
            else:
                next_col = least_covariance(mps, map(lambda r: r[0], answers), keys_to_skip)
                response = get_response_for_key(next_col)
        return json.dumps(response)
    index.exposed = True

class Root(object): pass
PATH = os.path.abspath(os.path.dirname(__file__))

cherrypy.config.update({'server.socket_port': 8000})
cherrypy.tree.mount(App(), '/api')
cherrypy.tree.mount(Root(), '/', config={
    '/': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': "%s/client/" % PATH,
        'tools.staticdir.index': 'index.html',
        },
    })
cherrypy.engine.start()
cherrypy.engine.block()

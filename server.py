import cherrypy, pymongo, json, os, itertools, re, urllib2, json, urllib
import numpy as np

depts = open('depts', 'r').read().split("\n")[1:-1]
mconn = pymongo.Connection()
db = mconn.rsparly2012
linkage = db.mpidlinktabledata
coll = db.data
coll_data = list(coll.find())
divisions = db.voterecorddivisions
vdm = db.voterecordvdm
voterecordvotetype = db.voterecordvotetype

CACHED_RESPONSES = {}
CACHED_RESPONSES_2 = {}

EPSILON = 1

def possible_answers(key): return coll.distinct(key)
def question_text(key):
    labels = {'party': 'What political party are they?',
              'mp_change': 'Have they only been an MP since the last election?',
              'party_change': 'Has their party only been in power since the last election?',
              'election_reason': 'Why were they elected?',
              'region': 'What region is their constituency in?',
              'expense': 'Do they claim higher than averages expenses?',
              'unemployment': 'Does their constituency have higher than average unemployment?',
              'turnout': 'Was their higher than average turnout at the last election?',
              'crime': 'Does their constituency have higher than average crime?',
              'has_government_post': 'Do they hold a government post?',
              'has_opposition_post': 'Do they hold a opposition post?',
              'is_sos': 'Are they a Secretary of State?',
              'is_ssos': 'Are they a Shadow Secreatary of State?',
              'is_mos': 'Are they a Minister of State?',
              'is_smos': 'Are they a Shadow Minister of State?',
              'gender': 'What gender are they?'}
    for dept in depts: labels["dept_%s" % dept] = "Do they work for/as %s" % dept
    if key in labels: return labels[key]
    else:
        div_title = divisions.find_one({'DivisionID': key})['DivTitle']
        return "How did they vote for %s (%d Labour MPs and %d Conservative MPs voted for this)?" % (parse_bill_name(div_title), labour_votes(key), tory_votes(key))
        raise "foo"

def parse_bill_name(name):
    name = " ".join(re.split('Act|Bill', name)[0:2])
    name = re.split('( ?,? ?Question)|;', name)[0]
    return name


def answer_texts(key):
    if key in ['party', 'region', 'gender', 'election_reason']: answers = possible_answers(key)
    elif key in ['mp_change', 'party_change', 'expense', 'unemployment', 'turnout', 'crime', 'has_government_post', 'has_opposition_post', 'is_sos', 'is_ssos', 'is_mos', 'is_smos'] or key in map(lambda dept: "dept_%s" % dept, depts): answers = ["Yes", "No"]
    else: answers = map(get_vote_type, possible_answers(key))
    answers.append("Not sure")
    return answers

def parse_answer(key, answer):
    if key in ['party', 'region', 'gender', 'election_reason']: return answer
    elif key in ["mp_change", "party_change", 'expense', 'unemployment', 'turnout', 'crime', 'has_government_post', 'has_opposition_post', 'is_sos', 'is_ssos', 'is_mos', 'is_smos'] or key in map(lambda dept: "dept_%s" % dept, depts): return answer == "Yes"
    else: return get_vote_id(answer)

def unique(a):
    indices = sorted(range(len(a)), key=a.__getitem__)
    indices = set(next(it) for k, it in itertools.groupby(indices, key=a.__getitem__))
    return [x for i, x in enumerate(a) if i in indices]

def cartesian_product2(arrays):
    la = len(arrays)
    arr = np.empty([len(a) for a in arrays] + [la])
    for i, a in enumerate(np.ix_(*arrays)):
        arr[...,i] = a
    return arr.reshape(-1, la)

def algorithm_u(ns, m):
    def visit(n, a):
        ps = [[] for i in xrange(m)]
        for j in xrange(n):
            ps[a[j + 1]].append(ns[j])
        return ps

    def f(mu, nu, sigma, n, a):
        if mu == 2:
            yield visit(n, a)
        else:
            for v in f(mu - 1, nu - 1, (mu + sigma) % 2, n, a):
                yield v
        if nu == mu + 1:
            a[mu] = mu - 1
            yield visit(n, a)
            while a[nu] > 0:
                a[nu] = a[nu] - 1
                yield visit(n, a)
        elif nu > mu + 1:
            if (mu + sigma) % 2 == 1:
                a[nu - 1] = mu - 1
            else:
                a[mu] = mu - 1
            if (a[nu] + sigma) % 2 == 1:
                for v in b(mu, nu - 1, 0, n, a):
                    yield v
            else:
                for v in f(mu, nu - 1, 0, n, a):
                    yield v
            while a[nu] > 0:
                a[nu] = a[nu] - 1
                if (a[nu] + sigma) % 2 == 1:
                    for v in b(mu, nu - 1, 0, n, a):
                        yield v
                else:
                    for v in f(mu, nu - 1, 0, n, a):
                        yield v

    def b(mu, nu, sigma, n, a):
        if nu == mu + 1:
            while a[nu] < mu - 1:
                visit(n, a)
                a[nu] = a[nu] + 1
            visit(n, a)
            a[mu] = 0
        elif nu > mu + 1:
            if (a[nu] + sigma) % 2 == 1:
                for v in f(mu, nu - 1, 0, n, a):
                    yield v
            else:
                for v in b(mu, nu - 1, 0, n, a):
                    yield v
            while a[nu] < mu - 1:
                a[nu] = a[nu] + 1
                if (a[nu] + sigma) % 2 == 1:
                    for v in f(mu, nu - 1, 0, n, a):
                        yield v
                else:
                    for v in b(mu, nu - 1, 0, n, a):
                        yield v
            if (mu + sigma) % 2 == 1:
                a[nu - 1] = 0
            else:
                a[mu] = 0
        if mu == 2:
            visit(n, a)
        else:
            for v in b(mu - 1, nu - 1, (mu + sigma) % 2, n, a):
                yield v

    n = len(ns)
    a = [0] * (n + 1)
    for j in xrange(1, m + 1):
        a[n - m + j] = j - 1
    return f(m, n, 0, n, a)

def find_results(query): return filter(lambda r: row_matches(r, query), coll_data)
def row_matches(r, query): return all(r[y] == x for y, x in query.items())

def find_result(answers):
    m = len(answers)
    k = 10
    t=5
    limit = int(1e2)

    best_epsilon_diff = float("inf")
    mps = []

    for i in range(0, t):
        if i >= m: continue
        A = map(dict, list(itertools.islice(itertools.combinations(answers.items(), m-i), limit)))

        P = map(lambda a: find_results(a), A)
        P = filter(lambda p: len(p) != 0, P)
        if len(P) == 0: continue

        B = sorted(P, key = lambda p: len(p), reverse=True)[0]
        print "%d: %d" % (m-i, len(B))
        epsilon_diff = len(B) - EPSILON
        if epsilon_diff < best_epsilon_diff:
            best_epsilon_diff = epsilon_diff
            mps = B

        if len(B) <= EPSILON:
            print "YES"
            M = sorted(B, key = lambda b: sum(map(lambda p: b in p, P)), reverse=True)[0]
            return [M, mps]

    print "Epsilon diff: %d" % best_epsilon_diff
    print any(map(lambda m: m['last_name'] == 'Cameron', mps))

    return [[], mps]

# def get_matching_mps(answers): return list(coll.find(dict(answers)))
# def get_cost_answers(mps): return len(mps)

def tory_votes(key): return get_votes(key, 'Conservative')
def labour_votes(key): return get_votes(key, 'Labour')

def get_votes(key, party): return vdm.find({"PARTY_NAME": party, "DivisionID": key, "VoteTypeID": '1'}).count()

def get_vote_type(vote_type): return voterecordvotetype.find_one({'VoteTypeID':vote_type})['VoteType']
def get_vote_id(vote_type): return voterecordvotetype.find_one({'VoteType':vote_type})['VoteTypeID']

def least_covariance(orig_data, cols_to_skip):
    if "party" not in cols_to_skip: return "party"
    modelen = np.bincount(map(len, orig_data)).argmax()
    orig_data = filter(lambda r: len(r) == modelen, orig_data)

    cols_to_skip.extend(['first_name', 'twfy_id', 'last_name', '_id'])
    cols_to_hash = ['party', 'mp_change', 'party_change', 'election_reason', 'region', 'gender', 'expense', 'unemployment', 'turnout', 'crime', 'has_government_post', 'has_opposition_post', 'is_sos', 'is_ssos', 'is_mos', 'is_smos']
    cols_to_hash.extend(map(lambda dept: "dept_%s" % dept, depts))
    cols_to_hash = set(cols_to_hash)

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
    covs = filter(lambda r: not r[0] in cols_to_skip, covs)

    use_hashed_cols = map(lambda col: not col in cols_to_skip, cols_to_hash)
    if any(use_hashed_cols): covs = filter(lambda r: r[0] in cols_to_hash, covs)

    covs = sorted(covs, key = lambda x: x[1], reverse=True)
    return covs[0][0]

def get_response_for_key(key): return {'question': question_text(key), 'question_id': key, 'answers': answer_texts(key)}

def get_answer_text(answer, key):
    pas = answer_texts(key)
    pas = filter(lambda pa: parse_answer(key, pa) == answer, pas)
    return pas[0]

def generate_result(result, answers, raw_answers):
    incorrect_answers = filter(lambda (k,v): result[k] != v, answers.items())
    incorrect_answers = map(lambda (k, vu): {'key': k, 'user':raw_answers[k], 'correct': get_answer_text(result[k], k)}, incorrect_answers)

    return "/f.html?%s" % urllib.urlencode({'id': result['twfy_id'], 'incorrect': incorrect_answers})

class App(object):
    def index(self, **answers):
        if hash(frozenset(answers.items())) in CACHED_RESPONSES: response = CACHED_RESPONSES[hash(frozenset(answers.items()))]
        else:
            raw_answers = {}
            keys_to_skip = list(answers.keys())
            for k in answers.keys():
                if answers[k] == "Not sure": del answers[k]
                else:
                    raw_answers[k] = answers[k]
                    answers[k] = parse_answer(k, answers[k])
            print "Not caching :("
            response = {}
            if len(answers) == 0:
                response = get_response_for_key('region')
            elif len(answers) > 25:
                response = {"error": "We failed"}
            else:
                result, mps = find_result(answers)
                if len(result) == 0:
                    next_col = least_covariance(mps, keys_to_skip)
                    response = get_response_for_key(next_col)
                else:
                    response = {"success": generate_result(result, answers, raw_answers)}
        CACHED_RESPONSES[hash(frozenset(answers.items()))] = response
        return json.dumps(response)

    def f(self, mpid, incorrect):
        if (mpid+incorrect) in CACHED_RESPONSES_2: response = CACHED_RESPONSES_2[(mpid+incorrect)]
        else:
            response = {'mpid': mpid}
            twfy_url = "http://www.theyworkforyou.com/api/getMPInfo?key=FqQ7HAE6VXorA8NhKHAmUeW5&id=%s" % mpid
            twfy_url2 = "http://www.theyworkforyou.com/api/getMP?key=FqQ7HAE6VXorA8NhKHAmUeW5&id=%s" % mpid
            twfy = json.load(urllib2.urlopen(twfy_url))
            twfy2 = json.load(urllib2.urlopen(twfy_url2))[0]
            old_incorrect = str(incorrect)
            incorrect = incorrect.replace("'", '"').replace(': u"', ': "')
            incorrect = json.loads(incorrect)

            incorrect_html = "\n".join(map(lambda ic: "<li>You said %s for '%s'; it's actually %s" % (ic.values()[0], question_text(ic.values()[1]), ic.values()[2]), incorrect))
            if len(incorrect) > 0: incorrect_html = "<h2>Incorrect fields</h2><ul>" + incorrect_html + "</ul>"

            details = [", ".join(map(lambda x: x['position'], twfy2['office'])) if 'office' in twfy2 else '', "%s, %s" % (twfy2['party'], twfy2['constituency'])]

            urls = [
                    "<div class='urls'><a href='" + twfy['wikipedia_url'] + "'>Wikipedia</a>",
                    "<div class='urls'><a href='" + twfy['guardian_mp_summary'] + "'>Guardian</a>",
                    "<div class='urls'><a href='" + twfy['bbc_profile_url'] + "'>BBC</a>",
                    "<div class='urls'><a href='" + twfy['expenses_url'] + "'>Expenses</a>"
                    ]
            urls = "\n".join(urls)

            photo = "http://www.theyworkforyou.com%s" % twfy2['image']
            photo_html = "<div id='image'><img src='" + photo + "'/></div>"

            html = photo_html + "<h1>" + twfy['name'] + "</h1>" + incorrect_html + "\n".join(map(lambda d: "<div class='detail'>%s</div>" % d, details)) + "<br/>" + urls

            response = {'html': html}
            CACHED_RESPONSES_2[(mpid+old_incorrect)] = response
        return json.dumps(response)

    index.exposed = True
    f.exposed = True

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

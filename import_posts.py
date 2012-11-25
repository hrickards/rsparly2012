import sqlite3, sys, pymongo, os, re
import xml.etree.ElementTree as ET

depts = open('depts', 'r').read().split("\n")[1:-1]

mconn = pymongo.Connection()
db = mconn.rsparly2012

def posts_include(posts, match):
    return any(map(lambda post: match in re.sub('\(.*\)', '', post), posts))

def parse_member(datum):
    opposition_posts = []
    government_posts = []
    g_posts = datum.find('GovernmentPosts')
    has_g_posts = len(g_posts) > 0
    if has_g_posts: government_posts = map(lambda x: x.find('Name').text, g_posts.findall("GovernmentPost"))
    o_posts = datum.find('OppositionPosts')
    has_o_posts = len(o_posts) > 0
    if has_o_posts: opposition_posts = map(lambda x: x.find('Name').text, o_posts.findall("OppositionPost"))
    their_posts = opposition_posts + government_posts
    their_depts = filter(lambda d: any(map(lambda p: d in p, their_posts)), depts)

    sos = has_g_posts and posts_include(government_posts, "Secretary of State")
    ssos = has_o_posts and posts_include(opposition_posts, "Secretary of State")
    mos = has_g_posts and posts_include(government_posts, "Minister of State")
    smos = has_o_posts and posts_include(opposition_posts, "Shadow Minister")
    return {'dods_id': datum.attrib['Dods_Id'], 'has_government_post': has_g_posts, 'has_opposition_post': has_o_posts, 'is_sos': sos, 'is_ssos': ssos, 'is_mos': mos, 'is_smos': smos, 'depts': their_depts}


tree = ET.parse('posts.xml')
data = map(parse_member, tree.findall('Member'))
coll = db['postsdata']
coll.drop()
coll.insert(data)

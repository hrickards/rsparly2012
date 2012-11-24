import sqlite3, sys, pymongo, os
import xml.etree.ElementTree as ET

mconn = pymongo.Connection()
db = mconn.rsparly2012

tree = ET.parse('all-members-2010.xml')
data = map((lambda r: r.attrib), tree.findall('member'))
coll = db['ms2010']
coll.drop()
coll.insert(data)

tree = ET.parse('all-members.xml')
data = map((lambda r: r.attrib), tree.findall('member'))
coll = db['ms']
coll.drop()
coll.insert(data)

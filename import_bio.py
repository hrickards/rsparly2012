import sqlite3, sys, pymongo, os
import xml.etree.ElementTree as ET

mconn = pymongo.Connection()
db = mconn.rsparly2012

def parse_member(datum):
    return {'dods_id': datum.attrib['Dods_Id'], 'gender': datum.find('Gender').text}


tree = ET.parse('biodata.xml')
data = map(parse_member, tree.findall('Member'))
coll = db['biodata']
coll.drop()
coll.insert(data)

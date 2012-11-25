import pymongo, sys, random, itertools

mconn = pymongo.Connection()
db = mconn.rsparly2012

expenses = db.mpsexpenses_2012
aggregation = db.expenses_aggregation

def parse_expenses(datum):
    total = sum(map(lambda x: float(x['Amount Claimed']), datum[1]))
    return {"MP's Name": datum[0], "Total Claimed": total}

data = expenses.find()
data = map(parse_expenses, itertools.groupby(data, key=lambda x: x["MP's Name"]))
aggregation.insert(data)

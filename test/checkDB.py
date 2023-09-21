import pymongo

client = pymongo.MongoClient("mongodb+srv://samuelnascimentof:rhBu0oB8SLVPZsYa@flowsensorscluster.hnybqa4.mongodb.net/Data?retryWrites=true&w=majority")
db = client.Data

cursor = db["sensor_lakes"].find()

for document in cursor:
    print(document)
from genericpath import exists
import requests
import json
from bs4 import BeautifulSoup
import pymongo
from pymongo import MongoClient, InsertOne
from collections import defaultdict
from datetime import datetime


# gets all documents from collection

client = pymongo.MongoClient('mongodb+srv://jgme4350:jgme4350@cluster0.nkehi.mongodb.net/dbit_control_db?retryWrites=true&w=majority')
db = client.app_main_database
collection = db["appsSummery"]
cursor = collection.find()
allApps = []
for document in cursor:
    listToAdd = []
    urlid = document['urlid']
    docid = document['id']
    appName = document['name'] 
    docServer = document['server']
    dataSize = document['datasize']
    appCounted = appName in allApps
    if(appCounted):
      continue
    else:
      allApps.append(appName)
    
    print(appName)
    
    # gets the json and breaks it apart
    # {"datetime":"2022-02-09 16:27:18","status":"Valid","devicetype":"Mobile","country_code":"ML","region_name":"Bamako Region","myip":"41.73.109.52","useragent":"Dalvik\/2.1.0 (Linux; U; Android 10; Infinix X655C Build\/QP1A.190711.020)}"
    if(urlid != "null"):
      mypage = requests.get(urlid)
      soup = BeautifulSoup(mypage.text, 'html.parser')
      myString = str(soup)
      myList = myString.split('%')
      myList.pop(0)
      jsonList = []
      for item in myList:
        json_object = json.loads(item) 
        dateTime = str(json_object["datetime"])
        json_object["date"] = dateTime[:10]
        jsonList.append(json_object)

      #grouping jsons by date
      groups = defaultdict(list)
      for obj in jsonList:
       groups[obj["date"]].append(obj)
      new_list = groups.values() 

      #getting the app colection
      appCollection = db[appName]

      #get last entry from
      lastCurser=appCollection.find().sort([('_id', -1)]).limit(1)
      amountOfDocuments = appCollection.count_documents({})

      if(amountOfDocuments==0):
        #create new day and add all entries to it
        #if no entries yet:
        for k,v in groups.items():
          newdoc = {'date':k,'datasize':0,'data':[]}
          print(appName+"inserting all data")
          appCollection.insert_one(newdoc)
          appCollection.update_many( { "date" : k },{"$set": { "data":v } })
          appCollection.update_one({"date": k}, {"$set": {"datasize": len(v)}})
        print("finished adding to"+appName)
      else:
        #got the last entry now need to add only what cam after
        lastDoc = lastCurser[0]
        lastDateStr = lastDoc['date']
        year = lastDateStr[:2]
        month = lastDateStr[5:7]
        day = lastDateStr[8:10]
        lastDateStr = day+"/"+month+"/"+year
        lastDate = datetime.strptime(lastDateStr, '%d/%m/%y')
        for k,v in groups.items():
          year = k[:2]
          month = k[5:7]
          day = k[8:10]
          KStr = day+"/"+month+"/"+year
          Kdate =  datetime.strptime(KStr, '%d/%m/%y')
          if(Kdate>lastDate):
            #if date is after we add all that came after
            newdoc = {'date':k,'datasize':0,'data':[]}
            print(appName+"inserting date "+KStr)
            appCollection.insert_one(newdoc)
            appCollection.update_many( { "date" : k },{"$set": { "data":v } })
            appCollection.update_one({"date": k}, {"$set": {"datasize": len(v)}})
  

     

client.close()



# coding: utf-8

import requests,json
from IPython.display import display
from datetime import datetime, timedelta
import pandas as pd
from bis2 import gc2
from bis import bis
from bis import sgcn

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["readAPI"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"],False)
thisRun["writeAPI"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"],True)
thisRun["commitToDB"] = True
thisRun["totalRecordsToProcess"] = 1
thisRun["totalRecordsProcessed"] = 0
thisRun["dateCheck"] = None

# Retrieve information from stored files on the SGCN base repository item
sb_sgcnCollectionItem = requests.get("https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5?format=json&fields=files").json()

for file in sb_sgcnCollectionItem["files"]:
    if file["title"] == "Configuration:Taxonomic Group Mappings":
        tgMappings = pd.read_table(file["url"], sep=",", encoding="utf-8")
    elif file["title"] == "Original 2005 SWAP National List for reference":
        swap2005 = pd.read_table(file["url"])

tgDict = {}
for index, row in tgMappings.iterrows():
    providedName = str(row["ProvidedName"])
    preferredName = str(row["PreferredName"])
    tgDict[providedName] = preferredName


numberWithoutTIRData = 1

q_currentTirSummaryRecords = "SELECT tirid FROM sgcn.tirsummary WHERE cachedate < '2017-11-08'"
r_currentTirSummaryRecords = requests.get(thisRun["writeAPI"]+"&q="+q_currentTirSummaryRecords).json()

for feature in r_currentTirSummaryRecords["features"]:

    q_recordToSearch = "SELECT id,registration,itis,worms,sgcn FROM tir.tir WHERE registration->>'source' = 'SGCN' AND itis IS NOT NULL AND worms IS NOT NULL AND sgcn IS NOT NULL AND id = "+str(feature["properties"]["tirid"])
    recordToSearch = requests.get(thisRun["writeAPI"]+"&q="+q_recordToSearch).json()
    
    numberWithoutTIRData = len(recordToSearch["features"])

    if numberWithoutTIRData == 1:
        thisRecord = {}
        thisRecord["registration"] = json.loads(recordToSearch["features"][0]["properties"]["registration"])
        thisRecord["itis"] = json.loads(recordToSearch["features"][0]["properties"]["itis"])
        
        # Stupid temporary fix. I didn't maintain backward compatibility in the code that processes ITIS documents while I'm in the middle of moving everything into a new architecture and a new way of dealing with API returns across the board.
        if "nameWInd" not in thisRecord["itis"]:
            thisRecord["itis"]["MatchMethod"] = "Not Matched"
        else:
            if thisRecord["itis"]["nameWInd"] == bis.cleanScientificName(thisRecord["registration"]["scientificname"]):
                thisRecord["itis"]["MatchMethod"] = "Exact Match"
            else:
                thisRecord["itis"]["MatchMethod"] = "Fuzzy Match"
        
        thisRecord["worms"] = json.loads(recordToSearch["features"][0]["properties"]["worms"])
        thisRecord["sgcn"] = json.loads(recordToSearch["features"][0]["properties"]["sgcn"])

        tirCommon = {}
        tirCommon["cachedate"] = datetime.utcnow().isoformat()

        tirCommon["tirid"] = recordToSearch["features"][0]["properties"]["id"]
        tirCommon["tirapi"] = thisRun["readAPI"]+"?q=SELECT * FROM tir.tir WHERE id="+str(tirCommon["tirid"])

        tirCommon["registeredname"] = bis.stringCleaning(thisRecord["registration"]["scientificname"])

        tirCommon["scientificname"] = tirCommon["registeredname"]
        tirCommon["commonname"] = None
        tirCommon["taxonomicgroup"] = "Other"
        tirCommon["taxonomicrank"] = "Unknown Taxonomic Rank"
        tirCommon["matchmethod"] = "Not Matched"
        tirCommon["acceptedauthorityapi"] = "Not Matched to Taxonomic Authority"
        tirCommon["acceptedauthorityurl"] = "Not Matched to Taxonomic Authority"
        
        if thisRecord["itis"]["MatchMethod"] != "Not Matched":
            tirCommon["scientificname"] = thisRecord["itis"]["nameWInd"]
            tirCommon["matchmethod"] = thisRecord["itis"]["MatchMethod"]
            tirCommon["taxonomicrank"] = thisRecord["itis"]["rank"]
            tirCommon["acceptedauthorityapi"] = "http://services.itis.gov/?q=tsn:"+str(thisRecord["itis"]["tsn"])
            tirCommon["acceptedauthorityurl"] = "https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value="+str(thisRecord["itis"]["tsn"])
        elif thisRecord["worms"]["MatchMethod"] != "Not Matched":
            tirCommon["scientificname"] = thisRecord["worms"]["valid_name"]
            tirCommon["matchmethod"] = thisRecord["worms"]["MatchMethod"]
            tirCommon["taxonomicrank"] = thisRecord["worms"]["rank"]
            tirCommon["acceptedauthorityapi"] = "http://www.marinespecies.org/rest/AphiaRecordByAphiaID/"+str(thisRecord["worms"]["AphiaID"])
            tirCommon["acceptedauthorityurl"] = "http://www.marinespecies.org/aphia.php?p=taxdetails&id="+str(thisRecord["worms"]["AphiaID"])
            
        if tirCommon["commonname"] is None:
            tirCommon["commonname"] = sgcn.getSGCNCommonName(thisRun["writeAPI"],bis.stringCleaning(thisRecord["registration"]["scientificname"]))

        if tirCommon["commonname"] is None and "commonnames" in list(thisRecord["itis"].keys()):
            for name in thisRecord["itis"]["commonnames"]:
                if name["language"] == "English" or name["language"] == "unspecified":
                    tirCommon["commonname"] = name["name"]
                    break
        
        if tirCommon["commonname"] is None:
            tirCommon["commonname"] = "no common name"
        else:
            tirCommon["commonname"] = bis.stringCleaning(tirCommon["commonname"])

        if tirCommon["matchmethod"] == "Not Matched" and "swap2005" in list(thisRecord["sgcn"].keys()) and thisRecord["sgcn"]["swap2005"] is True:
            tirCommon["matchmethod"] = "Legacy Match"
            tirCommon["acceptedauthorityapi"] = "https://www.sciencebase.gov/catalog/file/get/56d720ece4b015c306f442d5?f=__disk__38%2F22%2F26%2F38222632f48bf0c893ad1017f6ba557d0f672432"
            tirCommon["acceptedauthorityurl"] = "https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5"

        try:
            tirCommon["taxonomicgroup"] = tgDict[thisRecord["sgcn"]["taxonomicgroup"]]
        except:
            tirCommon["taxonomicgroup"] = thisRecord["sgcn"]["taxonomicgroup"]

        display (tirCommon)
        if thisRun["commitToDB"]:
            q_tirCommon = "UPDATE sgcn.tirsummary SET cachedate = '"+tirCommon["cachedate"]+"',             registeredname = '"+tirCommon["registeredname"]+"',             scientificname = '"+tirCommon["scientificname"]+"',             commonname = '"+tirCommon["commonname"]+"',             taxonomicgroup = '"+tirCommon["taxonomicgroup"]+"',             taxonomicrank = '"+tirCommon["taxonomicrank"]+"',             tirapi = '"+tirCommon["tirapi"]+"',             matchmethod = '"+tirCommon["matchmethod"]+"',             acceptedauthorityapi = '"+tirCommon["acceptedauthorityapi"]+"',             acceptedauthorityurl = '"+bis.stringCleaning(tirCommon["acceptedauthorityurl"])+"'             WHERE tirid = "+str(tirCommon["tirid"])
            print (requests.get(thisRun["writeAPI"]+"&q="+q_tirCommon).json())
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


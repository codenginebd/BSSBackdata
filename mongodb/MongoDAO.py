import time
import datetime
import csv
from pymongo import MongoClient
from locale import str

#HOST_NAME = "localhost"
#PORT = 27017
PRIMARY_KEY = "email"
CSV_FILE_NAME = "data.csv"

"""These are error codes that will be returned."""
ERROR_CODES = {
               'SUCCESS':0,
               'DATABASE_NOT_CONNECTED':1001,
               'VALUE_ERROR':1002,
               'TYPE_ERROR':1003,
               'UNKNOWN_ERROR':1004,
               'INSERT_FAILED':1005,
               'UPDATE_FAILED':1006,
               'DELETE_FAILED':1007,
               'WRONG_PARAMETER_TYPE_PASSED':1008,
               'PARAMETER_PASSED_WITH_NONE':1009,
               'NOT_FOUND':1010,
               'WEB_SERVICE_EXCEPTION':1011
               }

"""Each And Every method will must follow the following response format while returning data."""
RESPONSE_STRUCTURE = {
                 'failure_indication':1,
                 'error_code':None,
                 'message':None,
                 'data':None
                 }

class MongoDAO:
    def __init__(self):
        try:
            
            """Read configuration file."""
            config = self.ReadConfiguration()
            if config is not None and type(config) is tuple:
                HOST_NAME,PORT = tuple(config)
            else:
                raise Exception("Configuration read failed.")
            
            self.mongoClient = MongoClient(HOST_NAME, PORT)
            self.database = self.mongoClient.datacarddb
            self.userCollectionBasic = self.database.datacardUserProfile
            self.userCollectionFullProfile = self.database.datacardUserFullProfile
            self.userProfilesLastRowCountTable = self.database.tableLastRowCount
            self.userProfileLastCrawledProfileIDTable = self.database.tableLastCrawledProfileId
            print "MongoDB initialization successful."
            self.connected = True
            self.InitLastRowCountTable()
            self.InitLastCrawledProfileIdTable()
        except Exception,exp:
            print "MongoDB initialization failed."
            self.connected = False
            
            
    def ReadConfiguration(self):
        try:
            """Read Configuration.xml file for database host and port name."""
            f = open("Configuration.xml","r")
            contents = f.read()
            f.close()
            
            """Now parse configurations."""
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(contents)
            
            hostName,port = None,None
            
            """Read the host name"""
            hostNameElement = soup.find("property",{"name":"host_name"})
            if hostNameElement is not None:
                hostName = hostNameElement.text.strip()
            portElement = soup.find("property",{"name":"port"})
            if portElement is not None:
                port = portElement.text.strip()
            if hostName is not None and port is not None:
                return (hostName,int(port))
        except Exception,exp:
            return None
            
    def DBConnected(self):
        return self.connected
    
    def ResetAllSettings(self):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                """Remove all data from all tables."""
                self.userCollectionBasic.remove()
                self.userCollectionFullProfile.remove()
                self.userProfilesLastRowCountTable.remove()
                self.userProfileLastCrawledProfileIDTable.remove()
                self.InitLastRowCountTable()
                self.InitLastCrawledProfileIdTable()
                
                RESPONSE['failure_indication'] = 0
                RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                RESPONSE['message'] = 'Successful'
                RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
    
    def GetErrorCodes(self):
        return ERROR_CODES

    def ReadCSVFile(self):
        dataList = []
        with open(CSV_FILE_NAME, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            counter = 0
            for eachRow in reader:
                if counter == 0:
                    counter += 1
                    continue
                dataList.append({"id":eachRow[0],
                                 "email":eachRow[1],
                                 "first_name":eachRow[2],
                                 "last_name":eachRow[3],
                                 "title":eachRow[4],
                                 "address_one":eachRow[5],
                                 "address_two":eachRow[6],
                                 "city":eachRow[7],
                                 "state":eachRow[8],
                                 "zip":eachRow[9],
                                 "country":eachRow[10],
                                 "phone_one":eachRow[11],
                                 "phone_two":eachRow[12],
                                 "birth_year":eachRow[13],
                                 "hs_grad_year":eachRow[14],
                                 "highest_education_level":eachRow[15],
                                 "us_citizen":eachRow[16],
                                 "military_affiliation":eachRow[17],
                                 "best_time_to_call":eachRow[18],
                                 "is_valid_email":eachRow[19],
                                 "date_added":eachRow[20],
                                 "date_updated":eachRow[21]})
        return dataList
    
    def AddPrimaryKeyToDataRowsBasicProfile(self,data):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if data is None:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('PARAMETER_PASSED_WITH_NONE')
                RESPONSE['message'] = 'Must be called with data.'
                RESPONSE['data'] = ''
            if self.DBConnected() is True:
                firstRowKeyResponse = self.ReadLastRowCountBasicProfile()
                if firstRowKeyResponse.get('failure_indication') == 0:
                    firstRowKeyInt = firstRowKeyResponse.get('data')
                    for eachData in data:
                        if eachData is not None and type(eachData) is dict:
                            firstRowKeyInt += 1
                            eachData['p_id'] = firstRowKeyInt
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = (firstRowKeyInt,data)
                else:
                    RESPONSE = firstRowKeyResponse
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def AddPrimaryKeyToDataRowsFullProfile(self,data):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if data is None:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('PARAMETER_PASSED_WITH_NONE')
                RESPONSE['message'] = 'Must be called with data.'
                RESPONSE['data'] = ''
            if self.DBConnected() is True:
                firstRowResponse = self.ReadLastRowCountFullProfile()
                if firstRowResponse.get('failure_indication') == 0:
                    firstRowKeyInt = firstRowResponse.get('data')
                    for eachData in data:
                        if eachData is not None and type(eachData) is dict:
                            firstRowKeyInt += 1
                            eachData['p_id'] = str(firstRowKeyInt)
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = (firstRowKeyInt,data)
                else:
                    RESPONSE = firstRowResponse
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def ReadLastRowCountBasicProfile(self):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                lastRowCountRowBasic = self.userProfilesLastRowCountTable.find_one({'profile':'basic'})
                if lastRowCountRowBasic is not None and type(lastRowCountRowBasic) is dict:
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = lastRowCountRowBasic.get("last_row_count")
                else:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('NOT_FOUND')
                    RESPONSE['message'] = 'No rows for last row count of basic profile has been found in the db.'
                    RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE  
        
    def ReadLastRowCountFullProfile(self):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                lastRowCountRowFullProfile = self.userProfilesLastRowCountTable.find_one({'profile':'full'})
                if lastRowCountRowFullProfile is not None and type(lastRowCountRowFullProfile) is dict:
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = lastRowCountRowFullProfile.get("last_row_count")
                else:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('NOT_FOUND')
                    RESPONSE['message'] = 'No rows for last row count of full profile has been found in the db.'
                    RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def InitLastRowCountTable(self):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                basicProfileLastRowCount = self.userProfilesLastRowCountTable.find_one({'profile':'basic'})
                fullProfileLastRowCount = self.userProfilesLastRowCountTable.find_one({'profile':'full'})
                if basicProfileLastRowCount is None:
                    self.UpdateBasicProfileLastRowCount(0)
                if fullProfileLastRowCount is None:
                    self.UpdateFullProfileLastRowCount(0)
                RESPONSE['failure_indication'] = 0
                RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                RESPONSE['message'] = 'Successful'
                RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def InitLastCrawledProfileIdTable(self):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                
                lastCrawledProfileIdRow = self.userProfileLastCrawledProfileIDTable.find_one({'profile':'basic'})
                if lastCrawledProfileIdRow is None:
                    self.UpdateLastCrawledProfileIdTable(0)
                RESPONSE['failure_indication'] = 0
                RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                RESPONSE['message'] = 'Successful'
                RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def ReadLastCrawledProfileId(self):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                lastCrawledProfileRow = self.userProfileLastCrawledProfileIDTable.find_one({'profile':'basic'})
                if lastCrawledProfileRow is not None and type(lastCrawledProfileRow) is dict:
                    profileId = lastCrawledProfileRow.get('last_crawled_profile_id')
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = profileId
                else:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('NOT_FOUND')
                    RESPONSE['message'] = 'No rows found for last crawl profile records.'
                    RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def UpdateBasicProfileLastRowCount(self,value):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                self.userProfilesLastRowCountTable.update({'profile':'basic'},{'$set':{'last_row_count':value}},True)
                RESPONSE['failure_indication'] = 0
                RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                RESPONSE['message'] = 'Successful'
                RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def UpdateFullProfileLastRowCount(self,value):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                self.userProfilesLastRowCountTable.update({'profile':'full'},{'$set':{'last_row_count':value}},True)
                RESPONSE['failure_indication'] = 0
                RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                RESPONSE['message'] = 'Successful'
                RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
    
    def UpdateLastCrawledProfileIdTable(self,value):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        
        try:
            if self.DBConnected() is True:
                self.userProfileLastCrawledProfileIDTable.update({'profile':'basic'},{'$set':{'last_crawled_profile_id':value}},True)
                RESPONSE['failure_indication'] = 0
                RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                RESPONSE['message'] = 'Successful'
                RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
    
    def StoreBulkBasicProfile(self,data):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                response = self.AddPrimaryKeyToDataRowsBasicProfile(data)
                if response.get('failure_indication') == 0:
                    responseData = response.get('data')
                    if type(responseData) is tuple:
                        lastRowCount,modifiedData = tuple(responseData)
                        self.userCollectionBasic.insert(modifiedData)
                        basicProfileLastRowcountUpdateResponse = self.UpdateBasicProfileLastRowCount(lastRowCount)
                        if basicProfileLastRowcountUpdateResponse.get('failure_indication') == 0:
                            RESPONSE['failure_indication'] = 0
                            RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                            RESPONSE['message'] = 'Successful'
                            RESPONSE['data'] = ''
                        else:
                            RESPONSE['failure_indication'] = 1
                            RESPONSE['error_code'] = ERROR_CODES.get('UPDATE_FAILED')
                            RESPONSE['message'] = 'Data inserted successfully but last row count update failed.'
                            RESPONSE['data'] = ''
                    else:
                        RESPONSE['failure_indication'] = 1
                        RESPONSE['error_code'] = ERROR_CODES.get('TYPE_ERROR')
                        RESPONSE['message'] = 'Adding primary key to data rows have failed.'
                        RESPONSE['data'] = ''
                else:
                    RESPONSE = response
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def StoreFullProfileSingle(self,fullProfile,lastCrawledProfileId):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                if fullProfile is None:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('PARAMETER_PASSED_WITH_NONE')
                    RESPONSE['message'] = 'No parameter has been passed.'
                    RESPONSE['data'] = ''
                elif type(fullProfile) is not dict:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('TYPE_ERROR')
                    RESPONSE['message'] = 'Parameter type error. Expected dictionary.'
                    RESPONSE['data'] = ''
                fullProfileLastRowCountResponse = self.ReadLastRowCountFullProfile()
                if fullProfileLastRowCountResponse.get('failure_indication') == 0:
                    fullProfileLastRowCount = fullProfileLastRowCountResponse.get('data')
                    if fullProfileLastRowCount is not None and type(fullProfileLastRowCount) is int:
                        fullProfileLastRowCount += 1
                        fullProfile['p_id'] = fullProfileLastRowCount
                        self.userCollectionFullProfile.insert(fullProfile)
                        self.UpdateLastCrawledProfileIdTable(int(lastCrawledProfileId))
                        self.UpdateFullProfileLastRowCount(fullProfileLastRowCount)
                        RESPONSE['failure_indication'] = 0
                        RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                        RESPONSE['message'] = 'Successful'
                        RESPONSE['data'] = ''
                    else:
                        RESPONSE['failure_indication'] = 1
                        RESPONSE['error_code'] = ERROR_CODES.get('TYPE_ERROR')
                        RESPONSE['message'] = 'Type error in getting full profile last row count response. Expected integer value.'
                        RESPONSE['data'] = ''
                else:
                    RESPONSE = fullProfileLastRowCountResponse
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
    
    def ReadFromCSVIntoMongoDB(self):
        data = self.ReadCSVFile()
        return self.StoreBulk(data, True)
        
    """This method takes user and limit as params. 
    If no user is given then it search for any user by the specified limit."""
    def ReadBasicProfile(self,queryFilter=None,limit=1):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            
            cursor = None
            if self.DBConnected() is True:
                if queryFilter is None:
                    cursor = self.userCollectionBasic.find().limit(limit)
                else:
                    cursor = self.userCollectionBasic.find(queryFilter).limit(limit)
                userCollection = []
                if cursor is not None:
                    for eachUser in cursor:
                        userCollection.append(eachUser)
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = userCollection
                else:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('NOT_FOUND')
                    RESPONSE['message'] = 'No data found in the database.'
                    RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
        
    def ReadBasicProfileUsingLimit(self,start,count):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            if self.DBConnected() is True:
                startRowId = int(start)
                endRowId = int(start)+int(count)
                searchQueryFilter = {'p_id':{'$gte':startRowId,'$lt':endRowId}}
                resultsCursor = self.userCollectionBasic.find(searchQueryFilter)
                results = []
                if resultsCursor is not None:
                    for eachResult in resultsCursor:
                        if eachResult is not None and type(eachResult) is dict:
                            results.append(eachResult)
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = results
                else:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('NOT_FOUND')
                    RESPONSE['message'] = 'No results found in the database.'
                    RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
    
    def ReadFullProfile(self,queryFilter=None,limit=10):
        RESPONSE = {
                    'failure_indication':1,
                    'error_code':None,
                    'message':None,
                    'data':None
                    }
        try:
            cursor = None
            if self.DBConnected() is True:
                if queryFilter is None:
                    cursor = self.userCollectionFullProfile.find().limit(limit)
                else:
                    cursor = self.userCollectionFullProfile.find(queryFilter).limit(limit)
                userCollection = []
                if cursor is not None:
                    for eachUser in cursor:
                        singleUserProfile = {}
                        for key,value in eachUser.items():
                            if key != "_id":
                                singleUserProfile[key] = value
                        userCollection.append(singleUserProfile)
                    RESPONSE['failure_indication'] = 0
                    RESPONSE['error_code'] = ERROR_CODES.get('SUCCESS')
                    RESPONSE['message'] = 'Successful'
                    RESPONSE['data'] = userCollection
                else:
                    RESPONSE['failure_indication'] = 1
                    RESPONSE['error_code'] = ERROR_CODES.get('NOT_FOUND')
                    RESPONSE['message'] = 'No results found in the database.'
                    RESPONSE['data'] = ''
            else:
                RESPONSE['failure_indication'] = 1
                RESPONSE['error_code'] = ERROR_CODES.get('DATABASE_NOT_CONNECTED')
                RESPONSE['message'] = 'Database not connected.'
                RESPONSE['data'] = ''
        except Exception,exp:
            RESPONSE['failure_indication'] = 1
            RESPONSE['error_code'] = ERROR_CODES.get('UNKNOWN_ERROR')
            RESPONSE['message'] = 'Exception occured. %s' % str(exp)
            RESPONSE['data'] = ''
        return RESPONSE
#print MongoDAO().ReadLastRowCountFullProfile()
from mongodb import *

class BSSMongoDBDriver:
	def __init__(self):
		self.database = MongoDAO()
	
	def UploadFullProfile(self,profile,lastCrawledProfileId):
		response = self.database.StoreFullProfileSingle(profile,int(lastCrawledProfileId))
		return response
	
	def DownloadBasicProfileUsingLimit(self,limit):
		lastCrawledProfileIdResponse = self.database.ReadLastCrawledProfileId()
		if lastCrawledProfileIdResponse.get('failure_indication') == 0:
			response = self.database.ReadBasicProfileUsingLimit(start=int(lastCrawledProfileIdResponse.get('data'))+1,count=limit)
			return response
		else:
			lastCrawledProfileIdResponse
			
#driver = BSSMongoDBDriver()
#print driver.DownloadBasicProfileUsingLimit(10)
#from facebook import *
#from LinkedIn import *
#from GooglePlus import *
#from BSSMongoDBDriver import *
#from libraries import *
#from Twitter import *
#from KLout import *
#from googleplus import *
import json, urllib2
#from BSSBrowser import *

#gFile = open("gplus.html","r")
#gPlusSource = gFile.read()
#
#fFile = open("facebook_data.json","r")
#fbSource = fFile.read()
#
#gPlusParser = GPlusParser(gPlusSource)
#facebookParser = FBCrawler()
#
#gPlusProfile = gPlusParser.ParseProfile()
#fbProfile = json.loads(fbSource)
#
#matcher = ProfileMatcher()
##print gPlusProfile
#print matcher.MatchGooglePlus(fbProfile,gPlusProfile)

data = {"site_id": "siteId123", "batch_id": "batchId123","data": [{"first_name":"Md Shariful Islam","last_name":"Sohel","email":"sohel_buet065@yahoo.com","facebook":"https://www.facebook.com/sharifulislamsohel/","linkedin":"https://www.linkedin.com/profile/view?id=36379560&authType=NAME_SEARCH&authToken=Xu6Q&trk=api*a231405*s2393+07*","twitter":"https://twitter.com/codenginebd"}]}
req = urllib2.Request("http://localhost:8080/api/1.0/upload/", data=json.dumps(data),headers={"Content-Type": "application/json"})
urllib2.urlopen(req).read()

#browser = BSSBrowser()
#twitter = Twitter()
#twitter.Login(browser,{"email":"codenginebd@gmail.com","password":"lapsso065lapsso065"})
#p = twitter.CrawlProfile(browser,{"link":"https://twitter.com/codenginebd"})
#print json.dumps(p,indent=4)

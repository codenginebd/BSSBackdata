#!/usr/bin/env python
#Required Libraries:
#selenium,chromedriver,simplejson
from Tkinter import *
from selenium import webdriver
from facebook import *
import threading
import time
import simplejson as json
from ConfigParser import *
import pdb
from BSSBrowser import *
from globals import *
from bs4 import BeautifulSoup
import re
from facebook import *
from LinkedIn import *
from GooglePlus import *
from BSSMongoDBDriver import *
from libraries import *
from Twitter import *
from KLout import *
import json

class BSSCrawler:
	def __init__(self):
		print "Initializing Crawler..."
		self.browser = BSSBrowser()
		self.parserBase = FIIParser()
		self.parser = FII_FBParser()
		self.fbGraph = FBGraph()
		self.fbOauth = FBOauth(self.browser)
		self.facebookCrawler = FBCrawler()
		self.linkedin = LinkedIn()
		self.googlePlus = FIIGooglePlus(self.browser)
		self.twitter = Twitter()
		self.klout = KLout()
		self.mongoDBDriver = BSSMongoDBDriver()
		self.merger = Merger()
		self.utility = Utility()
		self.matcher = ProfileMatcher()
	def Authenticate(self):
		facebookLoginStatus,linkedInLoginStatus,googleLoginStatus,twitterLoginStatus,kloutLoginStatus = False,False,False,False,False
		"""Authorize Facebook"""
		fbLoginCredentials = {"email":FIIConstants.Credentials.Facebook.EMAIL,"password":FIIConstants.Credentials.Facebook.PASSWORD}
		self.browser.OpenURL(FIIConstants.FACEBOOK_ROOT_URL)
		facebookLoginStatus = self.browser.LoginFacebook(fbLoginCredentials)
		
		if facebookLoginStatus is True:
			passwordElement = self.browser.CheckIfPasswordElementExistsInFacebook()
			if passwordElement is not None:
				return False
		else:
			return False
		""" Authorize LinkedIn """
		linkedInCredentials = {"email":FIIConstants.Credentials.LinkedIn.EMAIL,"password":FIIConstants.Credentials.LinkedIn.PASSWORD}
		linkedInLoginStatus = self.linkedin.Authorize(self.browser,linkedInCredentials)
		
		if linkedInLoginStatus is True:
			linkedInPasswordBox = self.browser.CheckIfPasswordElementExistsInLinkedIn()
			if linkedInPasswordBox is not None:
				return False
		else:
			return False
		"""Authorize Google Plus"""
		googlePlusLoginCredentials = {"email":FIIConstants.Credentials.Google.EMAIL,"password":FIIConstants.Credentials.Google.PASSWORD}
		googleLoginStatus = self.googlePlus.Authenticate(googlePlusLoginCredentials)
		
		if googleLoginStatus is True:
			googlePasswordButton = self.browser.CheckIfPasswordElementExistsInGoogle()
			if googlePasswordButton is not None:
				return False
		else:
			return False
		
		"""Authorize Twitter"""
		twitterLoginCredentials = {"email":FIIConstants.Credentials.Twitter.EMAIL,"password":FIIConstants.Credentials.Twitter.PASSWORD}
		twitterLoginStatus = self.twitter.Login(self.browser,twitterLoginCredentials)
		
		if twitterLoginStatus is True:
			twitterPasswordElement = self.browser.CheckIfTwitterPasswordElementExists()
			if twitterPasswordElement is not None:
				return False
		else:
			return False
		
		"""Authorize KLout"""
		kloutLoginCredentials = {"email":FIIConstants.Credentials.KLout.EMAIL,"password":FIIConstants.Credentials.KLout.PASSWORD}
		kloutLoginStatus = self.klout.Login(self.browser,kloutLoginCredentials)
		
		if kloutLoginStatus is True:
			passwordBoxKlout = self.browser.CheckIfPasswordElementExistsInKLout()
			if passwordBoxKlout is not None:
				return False
		else:
			return False
		
		return True
		
	def Run(self):
		LIMIT = 50
		accessToken = ""
		try:
			authenticationStatus = self.Authenticate()
			print authenticationStatus
			if authenticationStatus is True:
				response = self.mongoDBDriver.DownloadBasicProfileUsingLimit(limit=LIMIT)
				if response.get('failure_indication') == 0:
					users = response.get('data')
					print len(users)
					if accessToken == "":
						accessToken = self.fbOauth.GetAccessToken()
					if accessToken is None or accessToken == "":
						raise Exception,"Getting access token for facebook failed!"
					while True:
						if users is not None:
							if type(users) is list:
								if len(users) == 0:
									print "Crawling has been done successfully."
									break
								for eachUser in users:
									email = eachUser.get("email")
									firstName = eachUser.get("first_name")
									lastName = eachUser.get("last_name")
									facebookProfileLink = eachUser.get('facebook')
									twitterLink = eachUser.get("twitter")
									linkedInProfileLink = eachUser.get("linkedin")
									facebookProfileBasic = None
									if facebookProfileLink is not None:
										facebookProfileBasic = {"link":facebookProfileLink}
									else:
										facebookProfileBasic = self.fbGraph.SearchUsersAndGetProfileLinks(self.browser,self.parserBase,self.utility,accessToken,eachUser)
									if facebookProfileBasic is not None and facebookProfileBasic == "SEARCH_FAILED_WITH_OAUTH_EXCEPTION":
										print "Facebook Profile Search failed. Maybe the netweok connection problem or user's facebook credentials has been changed. Exiting program..."
										return
										
									facebookProfile,linkedInProfile,googlePlusProfile,twitterProfile,kloutProfile,completeProfile = None,None,None,None,None,None
									if facebookProfileBasic is not None and type(facebookProfileBasic) is dict and facebookProfileBasic.get('link') is not None and facebookProfileBasic.get('link') != "":
										facebookProfile = self.facebookCrawler.CrawlProfile(self.browser,self.parser,facebookProfileBasic)
										
										if facebookProfile == "LOGIN_REQUIRED":
											print "Facebook login is Required to proceed. Exiting program..."
											return
										
				#						f = open("facebook_data.json","w")
				#						f.write(json.dumps(facebookProfile))
										
										facebookProfileTemp = None
										if facebookProfile is None:
											facebookProfile = {"email":email}
										else:
											facebookProfileTemp = facebookProfile
											facebookProfile["email"] = email
										userFirstName = eachUser.get("first_name") if eachUser.get("first_name") is not None else ""
										userLastName = eachUser.get("last_name") if eachUser.get("last_name") is not None else ""
										userFullName = userFirstName+userLastName
										googlePlusProfileBasic = {}
										profileMatched = False
										if userFullName != "" and facebookProfileTemp is not None:
											googlePlusProfileBasic["full_name"] = userFullName
											googlePlusSearchResult = self.googlePlus.SearchPeople(googlePlusProfileBasic)
											if googlePlusSearchResult == "SEARCH_FAILED":
												print "Search failed in google plsu. May be the network is down or the user need to looged in to search people."
												return
											if googlePlusSearchResult is not None:
												for eachSearchResult in googlePlusSearchResult:
													if eachSearchResult is not None and eachSearchResult.get("profile_link") is not None and eachSearchResult.get("profile_link") != "":
														googlePlusProfileBasic["link"] = eachSearchResult.get("profile_link")
														googlePlusProfile = self.googlePlus.CrawlProfile(googlePlusProfileBasic)
														"""Match google plus profile."""
														profileMatched = self.matcher.MatchGooglePlus(facebookProfile,googlePlusProfile)
														if profileMatched is True:
															break
										if profileMatched is False:
											googleplusProfile = None
									else:
										facebookProfile = {"email":email}
									if linkedInProfileLink is not None:
										linkedInProfile = self.linkedin.CrawlProfile(self.browser,linkedInProfileLink)
										if linkedInProfile == "LOGIN_REQUIRED":
											print "Linkedin login required. Exiting program..."
											return
									
									twitterName = None
									
									if twitterLink is not None:
										twitterName = twitterLink[twitterLink.rindex("/")+1:len(twitterLink)]
										twitterProfile = self.twitter.CrawlProfile(self.browser,{"link":twitterLink})
										if twitterProfile == "LOGIN_REQUIRED":
											print "Twitter login required. Exiting program..."
											return
									
									if twitterName is not None:
										kloutProfile = self.klout.CrawlKLoutProfile(self.browser,{"twitter_user_name":twitterName})
										if kloutProfile == "LOGIN_REQUIRED":
											print "KLout login required. Exiting program..."
											return
										
									print "Found complete profile "
									completeProfile = self.merger.Merge(facebookProfile,linkedInProfile,googlePlusProfile,twitterProfile,kloutProfile)
									if completeProfile is not None and type(completeProfile) is dict:
										if twitterLink is not None:
											completeProfile["twitter_profile_link"] = twitterLink
										if linkedInProfileLink is not None:
											completeProfile["linkedin_profile_link"] = linkedInProfileLink
										basicInfoCity = eachUser.get("city")
										if basicInfoCity is not None:
											if completeProfile.get("living_info") is not None:
												completeProfile.get("living_info")["city"] = basicInfoCity
										basicInfoState = eachUser.get("state")
										if basicInfoState is not None:
											if completeProfile.get("living_info") is not None:
												completeProfile.get("living_info")["state"] = basicInfoState
										basicInfoZip = eachUser.get("zip")
										if basicInfoZip is not None:
											if completeProfile.get("living_info") is not None:
												completeProfile.get("living_info")["zip"] = basicInfoZip
										basicInfoAddressOne = eachUser.get("address1")
										if basicInfoAddressOne is not None and basicInfoAddressOne != "null":
											if completeProfile.get("living_info") is not None:
												completeProfile.get("living_info")["address_one"] = basicInfoAddressOne
										basicInfoAddressTwo = eachUser.get("address2")
										if basicInfoAddressTwo is not None and basicInfoAddressTwo != "null":
											if completeProfile.get("living_info") is not None:
												completeProfile.get("living_info")["address_two"] = basicInfoAddressTwo
										basicInfoContactPhone = eachUser.get("phone1")
										if basicInfoContactPhone is not None:
											if completeProfile.get("contact_info") is not None:
												completeProfile.get("contact_info")["contact_phone"] = basicInfoContactPhone
										basicInfoHSGradYear = eachUser.get("hs_grad_year")
										if basicInfoHSGradYear is not None:
											if completeProfile.get("general_info") is not None:
												completeProfile.get("general_info")["hs_grad_year"] = basicInfoHSGradYear
					#				print completeProfile
									"""Saving will be done here."""
									if completeProfile is not None and type(completeProfile) is dict:
										if eachUser is not None:
											siteId = eachUser.get("site_id")
											batchId = eachUser.get("batch_id")
											if siteId is not None:
												completeProfile["site_id"] = siteId
											if batchId is not None:
												completeProfile["batch_id"] = batchId
											updateTime = datetime.datetime.utcnow()
											completeProfile["last_update_time"] = json.dumps(updateTime,default=Utility.datetimeToTimeMillis)
											
									f = open("full_profile_data.json","w")
									f.write(json.dumps(completeProfile,sort_keys=False, indent=4))
								
											
									lastCrawledProfileId = eachUser.get('p_id')
									print "Uploading full profile to database."		
									self.mongoDBDriver.UploadFullProfile(completeProfile,lastCrawledProfileId)
								"""Processing has been done for the list of users."""
								response = self.mongoDBDriver.DownloadBasicProfileUsingLimit(limit=LIMIT)
								if response.get('failure_indication') == 0:
									users = response.get('data')
									if accessToken == "":
										accessToken = self.fbOauth.GetAccessToken()
										if accessToken is None or accessToken == "":
											raise Exception,"Getting access token for facebook failed!"
								
						else:
							print "Number of users found to crawl is 0. Exiting..."
							break
				else:
					print "Database read failed with error code "+str(response.get('error_code'))+". Exiting..."
					return
			else:
				print "Authentication Failed.."
		except Exception,exp:
			print "Exception occured."+str(exp)
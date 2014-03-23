from twitter import *

class Twitter:
    def __init__(self):
        self.parser = TwitterParser()
        
    def Login(self,browser,loginCredentials):
        loginUrl = "https://twitter.com/"
        try:
            browser.OpenURL(loginUrl)
            return browser.LoginTwitter(loginCredentials)
        except Exception,exp:
            return False
        
    def CrawlProfile(self,browserRef,user):
        browser = browserRef
        twitterProfileUrl = user.get("link")
        twitterFavoritesUrl = twitterProfileUrl+"/favorites"
        twitterListUrl = twitterProfileUrl+"/lists"
        browser.OpenURL(twitterProfileUrl)
        profilePageSource = browser.GetPage()
        userFullName = self.parser.ParseFullName(profilePageSource)
        userScreenName = self.parser.ParseTwitterScreenName(profilePageSource)
        userInfo = self.parser.ParseAboutBioFromCover(profilePageSource)
        userLocation = self.parser.ParseLocation(profilePageSource)
        personalBlog = self.parser.ParsePersonalBlog(profilePageSource)
        followActivityCount = self.parser.ParseFollowActivitiesCount(profilePageSource)
        tweets = self.parser.ParseTweets(profilePageSource)
        browser.OpenURL(twitterFavoritesUrl)
        favoritesPageSource = browser.GetPage()
        favorites = self.parser.ParseFavorites(favoritesPageSource)
        browser.OpenURL(twitterListUrl)
        
        twitterPasswordBox = browser.CheckIfTwitterPasswordElementExists()
        if twitterPasswordBox is not None:
            return "LOGIN_REQUIRED"
        
        listPageSource = browser.GetPage()
        twitterList = self.parser.ParseLists(listPageSource)
        """Now make the profile"""
        twitterProfile = {}
        if userFullName is not None:
            twitterProfile["twitter_full_name"] = userFullName
        if userScreenName is not None:
            twitterProfile["twitter_screen_name"] = userScreenName
        if userInfo is not None:
            twitterProfile["twitter_user_bio"] = userInfo
        if userLocation is not None:
            twitterProfile["twitter_location"] = userLocation
        if type(followActivityCount) is dict:
            twitterProfile = dict(twitterProfile.items()+followActivityCount.items())
        twitterProfile["twitter_tweets"] = tweets
        twitterProfile["twitter_favorites"] = favorites
        twitterProfile["twitter_lists"] = twitterList
        return twitterProfile
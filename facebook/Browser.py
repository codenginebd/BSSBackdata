from selenium import webdriver
import time
class WebBrowser:
	def __init__(self):
		self.browser = webdriver.Chrome()
	def OpenURL(self,url):
		self.browser.get(url)
	def GetPage(self):
		self.page = self.browser.page_source
		encodedStr = self.page.encode("ascii","xmlcharrefreplace") 
		return encodedStr
	def FindMoreLinkedElements(self):
		elements = self.browser.find_elements_by_class_name("mediaRowRevealer")
		return elements
	def ClickElement(self,element):
		if element is not None:
			try:
				element.click()
				time.sleep(10)
				return True
			except Exception,e:
				print "Click Exception: "+str(e)
				return False
	def ClickOnMoreInfoElements(self):
		elements = self.FindMoreLinkedElements()
		for anElement in elements:
			if anElement is not None:
				try:
					anElement.click()
				except Exception,e:
					pass
		time.sleep(6)
	def FindLikesYearLinkElement(self):
		elements = self.browser.find_elements_by_class_name("prl")
		return elements
		#WebDriverWait(browser, 6).until(lambda driver : driver.find_element_by_xpath("//div[@class='mediaRowWrapper']/div[@class='mediaRowWrapper']"))
	def LoginFacebook(self,loginCredentials):
		email = loginCredentials["email"]
		password = loginCredentials["password"]
		emailField = self.browser.find_element_by_id("email")
		emailField.send_keys(email)
		passwordField = self.browser.find_element_by_id("pass")
		passwordField.send_keys(password)
		submitButton = self.browser.find_element_by_id("loginbutton")
		submitButton.click()
	def LoginLinkedIn(self,loginCredentials):
		email = loginCredentials["email"]
		password = loginCredentials["password"]
		try:
			emailField = self.browser.find_element_by_id("session_key-login")
			emailField.send_keys(email)
			passwordField = self.browser.find_element_by_id("session_password-login")
			passwordField.send_keys(password)
			time.sleep(6)
			submitButton = self.browser.find_element_by_id("signin")
			submitButton.click()
		except Exception,exp:
			pass
	def LoginGoogle(self,loginCredentials):
		email = loginCredentials["email"]
		password = loginCredentials["password"]
		try:
			emailField = self.browser.find_element_by_id("Email")
			emailField.send_keys(email)
			passwordField = self.browser.find_element_by_id("Passwd")
			passwordField.send_keys(password)
			signInButton = self.browser.find_element_by_id("signIn")
			signInButton.click()
		except Exception,exp:
			pass
	def FindElementByName(self,elementName):
		element = self.browser.find_element_by_name(elementName)
		return element
	def FindElementById(self,elementId):
		element = self.browser.find_element_by_id(elementId)
		return element
	def ExecuteScriptAndWait(self,code):
		self.browser.execute_script(code)
		time.sleep(6)
	def GetPageURL(self):
		return self.browser.current_url
	def Close(self):
		self.browser.close()
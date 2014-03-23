from bottle import *
from mongodb import *
import json

dao = MongoDAO()

@route('/api/1.0/profile/<site_id>/<batch_id>/<lmt:int>')
def GetUserFullProfile(site_id,batch_id,lmt=20):
	targetProfileHints = {"site_id":site_id,"batch_id":batch_id}
	usersProfileResponse = dao.ReadFullProfile(queryFilter=targetProfileHints,limit=lmt)
	return template(json.dumps(usersProfileResponse))

@route('/api/1.0/upload', method='POST')
def UploadBasicProfiles():
	RESPONSE = {
                'failure_indication':1,
                'error_code':None,
                'message':None,
                'data':None
                }
	try:
		postData = request.body.read()
		postDataDict = json.loads(postData)
		if postDataDict is not None and type(postDataDict) is dict:
			site_id = postDataDict.get("site_id")
			batch_id = postDataDict.get("batch_id")
			data = postDataDict.get("data")
			updateTime = datetime.datetime.utcnow()
			modifiedData = []
			if type(data) is list:
				for eachData in data:
					if eachData is not None and type(eachData) is dict:
						eachData["last_update_time"] = updateTime
						eachData["site_id"] = site_id
						eachData["batch_id"] = batch_id
						modifiedData.append(eachData)
				RESPONSE = dao.StoreBulkBasicProfile(modifiedData)
			else:
				RESPONSE['failure_indication'] = 0
				RESPONSE['error_code'] = 1003
				RESPONSE['message'] = 'JSON data type error. Expected json array found json dictionary.'
				RESPONSE['data'] = ''
		else:
			RESPONSE['failure_indication'] = 1
			RESPONSE['error_code'] = 1011
			RESPONSE['message'] = 'Wrong parameter passed in web service.'
			RESPONSE['data'] = ''
	except Exception,exp:
		RESPONSE['failure_indication'] = 1
		RESPONSE['error_code'] = 1004
		RESPONSE['message'] = 'Exception occured. %s' % str(exp)
		RESPONSE['data'] = ''
	return RESPONSE
    



@error(404)
def Error404(error):
	return "Sorry! The requested resources are not available."

@route('/api/1.0/error_codes')
def GetErrorCodes():
	return dao.GetErrorCodes()


run(host='localhost', port=8080, debug=True)
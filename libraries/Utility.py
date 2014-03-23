class Utility:
    def __init__(self):
        pass
    @staticmethod
    def datetimeToTimeMillis(obj):
        """Default JSON serializer."""
        import calendar, datetime
    
        if isinstance(obj, datetime.datetime):
            if obj.utcoffset() is not None:
                obj = obj - obj.utcoffset()
        millis = int(
            calendar.timegm(obj.timetuple()) * 1000 +
            obj.microsecond / 1000
        )
        return millis
    @staticmethod
    def ReadFile(fileName,mode="r"):
        try:
            f = open(fileName,mode)
            contents = f.read()
            return contents
        except Exception,exp:
            return None
    
#print json.dumps(datetime.datetime.now(), default=Utility.datetimeToTimeMillis)
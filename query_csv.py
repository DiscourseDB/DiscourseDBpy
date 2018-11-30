import sys
import requests
import urllib
import urllib3
from requests.auth import HTTPBasicAuth
import json
import csv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DiscourseDB:
    """Query DiscourseDB's REST interface and extract data.

    This assumes you've set a password on your account; if you are using
    Google to log in, this is *not* your google password, it's a separate
    DiscourseDB password feature.  See project "discoursedb-core/user-management"
    for tools for setting this password; currently only an admin may set
    or change passwords

    Example use is at the bottom of this file.  You must create a named
    selection in the data browser by hand, then this program can download
    the data.    
    """

    def __init__(self, user, password):
        self.user= user
        self.password = password
        self.service = "https://erebor.lti.cs.cmu.edu"
        self.db = None

    def set_db(self, db=None):
        """Limit interface to a particular database; or set to None to query across databases"""
        self.db = db

    def _upload(self, endpoint, params={}, filename=None, fileparam="file"):
        assert filename is not None, "No upload file specified"
        basic = HTTPBasicAuth(self.user,self.password)
        files = {fileparam: open(filename,"rb")}
        return requests.post("%s/%s" % (self.service, endpoint), data = {}, auth=basic, verify=False, params=params, files=files, stream=True, headers={'Accept-Encoding': 'gzip, deflate, br', "user": "cbogartdenver@gmail.com"}) 
        print("Posted")

    def _request(self, endpoint, params=None, parsejson = True):
        basic = HTTPBasicAuth(self.user,self.password)
        answer = requests.get("%s/%s" % (self.service, endpoint), auth=basic, verify=False, params=params)
        try:
            if parsejson:
                return json.loads(answer.text)
            else:
                return answer.text
        except Exception as e:
            print("Error querying discoursedb: ", e)
            print("    Server returned: ", answer.text)
            return answer.text 

    def list_saved_queries(self):
        url = "browsing/prop_list?ptype=query" 
        self.queries = self._request(url)
        if self.db is None:
            return [entry["propName"] for entry in self.queries]
        else:
            return [entry["propName"] for entry in self.queries 
                  if json.loads(entry["propValue"])["database"] == self.db]

    def query_literal(self, q):
        """Given a query name, returns a string representing the named query"""
        for entry in self.queries:
            if entry["propName"] == q:
                return entry["propValue"]

    def query_content(self, q):
        """Given a query name, returns a data structure representing the named query"""
        return json.loads(self.query_literal(q))

    def dump_query(self, q):
        """Given a query name, pretty-prints the query to stdout"""
        print(json.dumps(self.query_content(q), indent=4))
    
    def download_by_parts(self, query, tofile):
        q = query.copy()
        append = False
        rowcount = 0
        dps = q["rows"]["discourse_part"]
        print("Breaking query into", len(dps),"parts")
        for dp in dps:
           q["rows"]["discourse_part"] = [dp]
           print("Downloading part", json.dumps(dp))
           rc = self.download(q, tofile, append=append)
           rowcount += rc
           print("    Got",rc,"rows, for a total of", rowcount)
           append=True

    def upload_annotated(self, fromfile):
        """Upload the annotated file"""
        return self._upload("browsing/action/database/%s/uploadLightside" % (self.db.replace("discoursedb_ext_",""),),
                       filename=fromfile, fileparam = "file_annotatedFileForUpload")

    def download_for_annotation(self, query, tofile):
        """Run the query and download to a file

        May fail for very large queries, in which case use download_huge.
        If append=True, omit the header line, and append rather than write
        Return the number of rows retrieved"""

        data = self._request("browsing/action/downloadLightsideQuery/for_annotation.csv", 
                       params={"query": json.dumps(query)}, parsejson=False)
        outf = open(tofile, "wb")
        try:
            outf.write(data.encode("utf-8"))
        except Exception as e:
            print(e)
        try:
            return len(list(csv.reader(data.encode("utf-8")))) -1
        except Exception as e:
            print (e)
            return None

    def download(self, query, tofile, append=False):
        """Run the query and download to a file

        May fail for very large queries, in which case use download_huge.
        If append=True, omit the header line, and append rather than write
        Return the number of rows retrieved"""

        data = self._request("browsing/action/downloadQueryCsvExpandible/discoursedb_data.csv", 
                       params={"query": json.dumps(query)}, parsejson=False)
        if append:
            outf = open(tofile, "ab")
            try:
                outf.write(("".join(data.split("\n",1)[-1:])).encode("utf-8"))
            except Exception as e:
                print(e)
        else:
            outf = open(tofile, "wb")
            try:
                outf.write(data.encode("utf-8"))
            except Exception as e:
                print(e)
        try:
            return len(list(csv.reader(data.encode("utf-8")))) -1
        except Exception as e:
            print (e)
            return None

if __name__ == "__main__":
    user = "FILL IN USERNAME"   # Your DiscourseDB user id, which is an email address
    password = "FILL IN PASSWORD" # Set this using the github.com/discoursedb-core/discoursedb-management project
    database = "FILL IN DATABASE"  
    ddb = DiscourseDB(user, password)
    ddb.set_db(database)
    print(ddb.list_saved_queries())
    q = ddb.list_saved_queries()[0]    # Download the first saved query
    ddb.download(ddb.query_content(q), "data_" + q + ".csv")

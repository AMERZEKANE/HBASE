# HBASE_en_python
### charger le CSV en HBASE
* import os
* os.system("cd bin/")
os.system("hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=',' -Dimporttsv.columns='HBASE_ROW_KEY,cf1:ename,cf1:designation,cf1:manager,cf2:hire_date,cf2:sal,cf2:deptno' datalab:emp_data /user/a4175704/les_fichier_csv/emp_data.csv")

### pour faire comminuquer Hbase avec un code python ta deux fichier "HBaseREST.py" et "HttpFS.py"

""" HBaseREST.py"""

#!pip install requests_kerberos

import sys
#sys.path.append("/home/cdsw/test_project001/hadoop_remote_library/")
import base64
import requests
import json
import xml.etree.ElementTree as ET

from requests_kerberos import HTTPKerberosAuth, OPTIONAL


HBASE_REST_URL = "http://acfsv817bna182p.bigdata4sg.saint-gobain.net:20550"


class HBaseREST:


  def __init__(self, url=HBASE_REST_URL, namespace=None):
    self.url = url
    self.namespace = namespace
    self.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    self._getHeaders = { "Accept": "text/xml" }
    self._putHeaders = { "Content-Type": "text/xml", "Accept": "text/xml" }


  def _getAPI(self, payload):
    r = requests.get(
      url=self.url + payload, auth=self.auth,
      headers=self._getHeaders
    )
    if r.status_code == 200:
      return ET.fromstring(r.content)
    raise Exception("Error: HBase REST API send HTTP Code: {}".format(r.status_code))


  def _putAPI(self, payload, data):
    r = requests.put(
      url=self.url + payload, auth=self.auth,
      headers=self._putHeaders, data=data
    )
    if r.status_code == 200:
      return True
    raise Exception("Error: HBase REST API send HTTP Code: {}".format(r.status_code))


  def version(self):
    return self._getAPI("/version/cluster").text


  def listTables(self):
    root = self._getAPI("/namespaces/{}/tables".format(self.namespace))
    return [child.attrib["name"] for child in root]


  def getRow(self, table, row):
    if self.namespace:
      root = self._getAPI("/{}:{}/{}".format(self.namespace, table, row))
    else:
      root = self._getAPI("/{}/{}".format(table, row))
    row = root[0]
    res = {}
    for cell in row:
      column = base64.b64decode(cell.attrib["column"])
      value = base64.b64decode(cell.text)
      res[column] = value
    return res


  def putCell(self, table, row, column, value):
    data = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="%s"><Cell column="%s">%s</Cell></Row></CellSet>' % (
      base64.b64encode(row),
      base64.b64encode(column),
      base64.b64encode(value)
    )
    if self.namespace:
      self._putAPI(
        "/{}:{}/{}/".format(self.namespace, table, row),
        data  
  ####    )
      
      
 """ HttpFS.py """
 
 #!pip install requests_kerberos

import requests

from requests_kerberos import HTTPKerberosAuth, OPTIONAL

HTTPFS_URL = "http://acfsv817bna182p.bigdata4sg.saint-gobain.net:14000/webhdfs/v1"

class HttpFS:
  
  
  def __init__(self, url=HTTPFS_URL):
    self.httpfs = url
    self.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)


  def getFile(self, hdfspath, localpath):
    if hdfspath[0] != '/':
      raise Exception("Error: hdfspath should be absolute (start with '/')")
    url = "{}{}".format(self.httpfs, hdfspath)
    params = { "op": "OPEN"}
    r = requests.get(
      url, auth=self.auth,
      params=params, stream=True
    )
    r.raise_for_status()
    with open(localpath, 'wb') as handle:
      for block in r.iter_content(4096):
          handle.write(block)
    return True
    
    
    #### exemple de validation 
 * !pip install requests_kerberos

#### ------------ #
#### HBASE Sample
#### ------------ #
* sys.path.append("/home/cdsw/test_project001/hadoop_remote_library/")
from HBaseREST import HBaseREST

hbase = HBaseREST(namespace="datalab")
hbase.version()
hbase.listTables()

* PUT in table "test_table",
* in row "1",
#### in column family "cf1",
#### in column "firstname"
#### the value "Jean"
hbase.putCell('test_table','500', 'cf1:firstname', 'Omar')

hbase.putCell("test_table", "500", "cf1:lastname", "Omar")
hbase.putCell("test_table", "500", "cf1:age", "50")
hbase.getRow("test_table", "400")
hbase.getRow("authentification",'1')


### ------------ #
### HDFS Getfile
### ------------ #

from HttpFS import HttpFS

httpfs = HttpFS()
httpfs.getFile("/tmp/myfile", "myfile") # reload left panel to see new created file


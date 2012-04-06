from xml.dom import minidom
import urllib2

class QuickBaseClient(object):

    def __init__(self, domain, dbid='main', ticket=None, username=None, password=None, ticket_hours=8, apptoken=None, verbose=False):
        self.domain = domain
        self.dbid = dbid
        self.ticket = ticket
        self.username = username
        self.password = password
        self.ticket_hours = ticket_hours
        self.apptoken = apptoken
        self.verbose = verbose

    def _authenticate(self):
        if not self.username or not self.password:
            raise RuntimeError("Authentication ticket failed")
        res = self.API_Authenticate(_do_auth=False,
                username=self.username, password=self.password,
                hours=self.ticket_hours)
        if 'errcode' in res and res['errcode'] != '0':
            raise RuntimeError("%s: %s" % (res['errtext'], res['errdetail']))
        else:
            self.ticket = res['ticket']

    def _request(self, action, _do_auth=True, **parameters_orig):
        parameters = parameters_orig.copy()

        doc = minidom.Document()
        api = doc.createElement("qdbapi")
        doc.appendChild(api)

        if 'dbid' in parameters:
            dbid = parameters.pop('dbid')
        else:
            dbid = self.dbid

        if _do_auth:
            if not self.ticket:
                self._authenticate()
            parameters['ticket'] = self.ticket

        if 'apptoken' in parameters:
            parameters['apptoken'] = self.apptoken

        for k,v in parameters.iteritems():
            e = doc.createElement(k)
            e.appendChild(doc.createTextNode(str(v)))
            api.appendChild(e)

        xml = doc.toxml()
        self._verb(xml)
        doc.unlink()

        url = 'https://%s/db/%s' % (self.domain, dbid)
        self._verb(url)
        req = urllib2.Request(url, xml,
                { 'quickbase-action' : action,
                  'content-type' : 'application/xml',
                })
        res = urllib2.urlopen(req)

        resxml = minidom.parse(res)
        self._verb(resxml.toxml())
        res = {}
        for node in resxml.firstChild.childNodes:
            n = node.nodeName
            if n == '#text':
                continue
            res[n] = node.firstChild.nodeValue
        res['xml'] = resxml
        self._verb(res)

        # check for auth failed and retry (once)..
        if _do_auth and res.get('errcode', None) == '4':
            self._authenticate()
            return self._request(action, _do_auth=False, **parameters_orig)

        return res

    def _verb(self, msg):
        if self.verbose:
            print msg

    def __getattr__(self, k):
        if not k.startswith('API_'):
            raise AttributeError(k)
        return lambda **kwargs : self._request(k, **kwargs)

class QuickBaseInterface(object):

    def __init__(self, qbc):
        self.qbc = qbc

class QuickBaseRoot(QuickBaseInterface):

    def list_apps(self):
        res = self.qbc.API_GrantedDBs()
        dbs_xml = res['xml']
        dbs = {}
        for info in dbs_xml.getElementsByTagName('dbinfo'):
            name = info.getElementsByTagName('dbname')[0].firstChild.nodeValue
            dbid = info.getElementsByTagName('dbid')[0].firstChild.nodeValue
            dbs[name] = QuickBaseDB(self.qbc, dbid)
        return dbs

    def get_app(self, dbname):
        res = self.qbc.API_FindDBByName(
                dbname=dbname, ParentsOnly=1)
        return QuickBaseDB(self.qbc, res['dbid'])

class QuickBaseDB(QuickBaseInterface):

    def __init__(self, qbc, dbid):
        QuickBaseInterface.__init__(self, qbc)
        self.dbid = dbid
        self._get_schema()

    def _get_schema(self):
        self.qbc.API_GetSchema(dbid=self.dbid, apptoken=True)


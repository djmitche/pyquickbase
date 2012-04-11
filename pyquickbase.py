from xml.dom import minidom
import urllib2
import logging

class QuickBaseClient(object):

    def __init__(self, domain, dbid='main', ticket=None, username=None, password=None, ticket_hours=8, apptoken=None):
        self.domain = domain
        self.dbid = dbid
        self.ticket = ticket
        self.username = username
        self.password = password
        self.ticket_hours = ticket_hours
        self.apptoken = apptoken

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

        if 'dbid' in parameters:
            dbid = parameters.pop('dbid')
        else:
            dbid = self.dbid
        log = logging.getLogger('pyquickbase.%s.%s' % (hex(id(self)), dbid))
        log.info('API call: ' + action)

        if _do_auth:
            if not self.ticket:
                self._authenticate()
            parameters['ticket'] = self.ticket

        if 'apptoken' in parameters:
            parameters['apptoken'] = self.apptoken

        doc = minidom.Document()
        api = doc.createElement("qdbapi")
        doc.appendChild(api)
        for k,v in parameters.iteritems():
            e = doc.createElement(k)
            e.appendChild(doc.createTextNode(str(v)))
            api.appendChild(e)

        xml = doc.toxml()
        doc.unlink()

        url = 'https://%s/db/%s' % (self.domain, dbid)
        log.debug('sending XML to %s:\n%s' % (url, xml))
        req = urllib2.Request(url, xml,
                { 'quickbase-action' : action,
                  'content-type' : 'application/xml',
                })
        res = urllib2.urlopen(req)

        resxml = minidom.parse(res)
        log.debug('received XML:\n%s' % (resxml,))
        res = {}
        for node in resxml.firstChild.childNodes:
            n = node.nodeName
            if n == '#text':
                continue
            fc = node.firstChild
            if fc:
                res[n] = fc.nodeValue
        res['xml'] = resxml
        log.debug('received Python:\n%r' % (res,))

        # check for auth failed and retry (once)..
        if _do_auth and res.get('errcode', None) == '4':
            self._authenticate()
            return self._request(action, _do_auth=False, **parameters_orig)

        return res

    def __getattr__(self, k):
        if not k.startswith('API_'):
            raise AttributeError(k)
        return lambda **kwargs : self._request(k, **kwargs)


class QuickBaseInterface(object):

    def __init__(self, qbc):
        self.qbc = qbc


class QuickBaseRoot(QuickBaseInterface):

    _apps_loaded = False
    def _load_apps(self):
        if self._apps_loaded:
            return
        res = self.qbc.API_GrantedDBs(excludeparents=0, withembeddedtables=0)
        dbs_xml = res['xml']
        self._apps_by_name = {}
        self._apps_by_dbid = {}
        for info in dbs_xml.getElementsByTagName('dbinfo'):
            name = info.getElementsByTagName('dbname')[0].firstChild.nodeValue
            dbid = info.getElementsByTagName('dbid')[0].firstChild.nodeValue
            app = QuickBaseApp(self.qbc, dbid)
            self._apps_by_name[name] = app
            self._apps_by_dbid[dbid] = app
        self._apps_loaded = True

    @property
    def apps_by_name(self):
        self._load_apps()
        return self._apps_by_name

    @property
    def apps_by_dbid(self):
        self._load_apps()
        return self._apps_by_dbid

    def get_app(self, dbname, dbid=None):
        "look up dbname, or just access the given dbid directly"
        if dbid:
            return QuickBaseApp(self.qbc, dbid)

        # search for it
        res = self.qbc.API_FindDBByName(
                dbname=dbname, ParentsOnly=1)
        return QuickBaseApp(self.qbc, res['dbid'])


class QuickBaseApp(QuickBaseInterface):

    def __init__(self, qbc, dbid):
        QuickBaseInterface.__init__(self, qbc)
        self.dbid = dbid

    _tables_loaded = False
    def _load_tables(self):
        if self._tables_loaded:
            return
        info = self.qbc.API_GetSchema(dbid=self.dbid, apptoken=True)['xml']
        self._tables_by_name = {}
        self._tables_by_dbid = {}
        for elt in info.getElementsByTagName('chdbid'):
            name = elt.getAttribute('name')
            name = name.replace('_dbid_', '')
            dbid = elt.firstChild.nodeValue
            tbl = QuickBaseTable(self.qbc, dbid)
            self._tables_by_name[name] = tbl
            self._tables_by_dbid[dbid] = tbl

    @property
    def tables_by_name(self):
        self._load_tables()
        return self._tables_by_name

    @property
    def tables_by_dbid(self):
        self._load_tables()
        return self._tables_by_dbid

    def get_table(self, tablename, dbid=None):
        "look up tablename in the app, or just access the given dbid directly"
        if dbid:
            return self.QuickBaseTable(self.qbc, dbid)

        # search for it
        return self.tables_by_name[tablename]


class QuickBaseTable(QuickBaseInterface):

    def __init__(self, qbc, dbid):
        QuickBaseInterface.__init__(self, qbc)
        self.dbid = dbid

    _schema_loaded = False
    def _load_schema(self):
        if self._schema_loaded:
            return
        info = self.qbc.API_GetSchema(dbid=self.dbid, apptoken=True)['xml']
        self._fields_by_name = {}
        self._fields_by_fid = {}
        for elt in info.getElementsByTagName('field'):
            fld = QuickBaseField(elt)
            self._fields_by_name[fld.name] = fld
            self._fields_by_fid[fld.fid] = fld

    @property
    def fields_by_name(self):
        self._load_schema()
        return self._fields_by_name

    @property
    def fields_by_dbid(self):
        self._load_schema()
        return self._fields_by_dbid

    def query(self, columns, condition):
        info = self.qbc.API_DoQuery(dbid=self.dbid, apptoken=True,
                query=condition, clist=columns, fmt='structured')['xml']
        records = info.getElementsByTagName('records')[0]
        return QuickBaseResultSet(records, self)


class QuickBaseField(object):

    def __init__(self, elt):
        self.fid = int(elt.getAttribute('id'))
        self.base_type = elt.getAttribute('base_type')
        self.field_type = elt.getAttribute('field_type')
        self.label = elt.getElementsByTagName('label')[0].firstChild.nodeValue
        self.name = elt.getAttribute('role') or self.label


class QuickBaseResultSet(object):

    def __init__(self, records_elt, table):
        self.records_elt = records_elt
        self.table = table

    def fetchall(self):
        for elt in self.records_elt.getElementsByTagName('record'):
            yield QuickBaseResultRow(elt, self.table)


class QuickBaseResultRow(object):

    def __init__(self, record_elt, table):
        self.record_elt = record_elt
        self.table = table
        self._parse()

    def _parse(self):
        fields = {}
        for elt in self.record_elt.getElementsByTagName('f'):
            id = int(elt.getAttribute('id'))
            val = elt.firstChild.nodeValue
            fields[id] = val
        self.fields = fields

    def __getitem__(self, k):
        if k in self.fields:
            return self.fields[k]
        # translate k to a fid
        if k in self.table.fields_by_name:
            fid = self.table.fields_by_name[k]
            rv = self.fields[k] = self.fields[fid]
            return rv
        raise KeyError('no such field %r' % (k,))

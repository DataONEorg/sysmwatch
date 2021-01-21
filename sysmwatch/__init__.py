import logging
import json
import psycopg2
import requests
import datetime
import dateparser

METACAT_DB = "metacat"
METACAT_HOST = "localhost"
METACAT_PORT = 5433
MAX_ROWS = 5000
ID_COLUMN = 0
MODIFIED_COLUMN = 1
UPLOADED_COLUMN = 2
ORIGIN_COLUMN = 3
FORMAT_COLUMN = 4

#SOLR_URL = "https://cn.dataone.org/cn/v2/query/solr/"
SOLR_URL = "http://localhost:8983/solr/search_core/select"
SOLR_BATCH_SIZE = 10
RESERVED_CHAR_LIST = [
    "+","-","&","|","!","(",")","{","}","[","]","^",'"',"~","*","?",":",
]
JSON_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
"""datetime format string for generating JSON content
"""

def datetimeToJsonStr(dt):
    if dt is None:
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        # Naive timestamp, convention is this must be UTC
        return f"{dt.strftime(JSON_TIME_FORMAT)}+0000"
    return dt.strftime(JSON_TIME_FORMAT)


def dtnow():
    """
    Get datetime for now in UTC timezone.

    Returns:
        datetime.datetime with UTC timezone

    Example:

        .. jupyter-execute::

           import igsn_lib.time
           print(igsn_lib.time.dtnow())
    """
    return datetime.datetime.now(datetime.timezone.utc)


def utcFromDateTime(dt, assume_local=True):
    # is dt timezone aware?
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        if assume_local:
            # convert local time to tz aware utc
            dt.astimezone(datetime.timezone.utc)
        else:
            # asume dt is in UTC, add timezone
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    # convert to utc timezone
    return dt.astimezone(datetime.timezone.utc)


def escapeSolrTerm(term):
    for c in RESERVED_CHAR_LIST:
        term = term.replace(c, r"\{}".format(c))
    return term

def _solrDateToDatetime(tstr):
    return dateparser.parse(tstr, settings={"TIMEZONE": "+0000", "RETURN_AS_TIMEZONE_AWARE":False})

def _getIndexDocs(S, pids):
    pid_list = []
    for pid in pids:
        pid_list.append(escapeSolrTerm(pid))
    params = {
        "wt":"json",
        "fl":"id,dateModified",
        "q": "id:(" + " || ".join(pid_list) + ")",
    }
    res = S.get(SOLR_URL, params=params)
    data = json.loads(res.text)
    _docs = data.get("response",{}).get("docs", [])
    docs = []
    for doc in _docs:
        docs.append({
            "id": doc["id"],
            "dateModified":_solrDateToDatetime(doc["dateModified"]),
        })
    return docs

def getIndexDocs(S, pids):
    docs = []
    for i in range(0, len(pids), SOLR_BATCH_SIZE):
        _pids = pids[i:i+SOLR_BATCH_SIZE]
        docs = docs + _getIndexDocs(S, _pids)
    return docs


def connectMetacat(port=METACAT_PORT, user_name="dataone_readonly", password=None):
    conn = psycopg2.connect(
        dbname=METACAT_DB,
        user=user_name,
        password=password,
        host=METACAT_HOST,
        port=port
    )
    return conn


def sysmetaDelta(conn, modified_since):
    '''
    List of [identifier, time] newer than modified_since
    Args:
        conn: database connection
        modified_since:

    Returns:
        list of all matching records, up to MAX_ROWS

    '''
    result = []
    sql = """SELECT guid, date_modified, date_uploaded, origin_member_node, object_format
    FROM systemmetadata 
    WHERE date_modified>=%s
    ORDER BY date_modified desc
    LIMIT %s;"""
    with conn.cursor() as cur:
        cur.execute(sql, (modified_since, MAX_ROWS,))
        result = cur.fetchall()
    return result


def nextModifiedDate(modified_since, previous_batch):
    if len(previous_batch) == 0:
        return modified_since
    candidate = previous_batch[0][MODIFIED_COLUMN]
    if candidate > modified_since:
        return candidate
    return modified_since


def getIdxDoc(pid, docs):
    for doc in docs:
        if doc['id'] == pid:
            return doc
    return None

def generateReport(conn, modified_since):
    '''
    Generates a dictionary reporting records modified and index status.

    Args:
        conn: Connection to postgres
        modified_since: datetime, earliest is > this

    Returns:
        dict

    '''
    report = {
        't_created':dtnow(),
        't_modified_since': modified_since,
        't_oldest_bad': modified_since,
        'records': []
    }
    record_template = {
        'ok': False,
        'pid': None,
        't_modified': None,
        't_uploaded': None,
        't_modified_idx': None,
        'origin_mn': None,
        'format_id': None
    }
    sysm_records = sysmetaDelta(conn, modified_since=modified_since)
    pids = []
    for rec in sysm_records:
        pids.append(rec[ID_COLUMN])
    session = requests.Session()
    idx_records = getIndexDocs(session, pids)
    #First record in response is newest
    t_oldest = sysm_records[0][MODIFIED_COLUMN]
    for rec in sysm_records:
        r = record_template.copy()
        r['pid'] = rec[ID_COLUMN]
        r['t_modified'] = rec[MODIFIED_COLUMN]
        r['t_uploaded'] = rec[UPLOADED_COLUMN]
        r['origin_mn'] = rec[ORIGIN_COLUMN]
        r['format_id'] = rec[FORMAT_COLUMN]
        doc = getIdxDoc(r['pid'], idx_records)
        if doc is not None:
            r['t_modified_idx'] = doc['dateModified']
            if r['t_modified'] == r['t_modified_idx']:
                r['ok'] = True
        if not r['ok']:
            if r['t_modified'] < t_oldest:
                t_oldest = r['t_modified']
        report['records'].append(r)
    report['t_oldest_bad'] = t_oldest
    return report
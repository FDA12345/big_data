from urllib.request import urlopen, Request, ProxyHandler, build_opener
from bs4 import BeautifulSoup, NavigableString
import re
import sys
import threading
from time import sleep, time
import pymysql.cursors
import io
import gzip
import random

g_MySqlHost = '127.0.0.1'
g_MySqlDb = 'spider'
g_MySqlUser = 'root'
g_MySqlPwd = ''
g_MySqlEncoding = 'utf8'
g_MySqlConnTimeout = 5
#g_LinksTblName = 'links'
g_LinksTblName = 'links_hashed'
#g_LinksPathTblName = 'links_path_raw'
g_LinksPathTblName = 'links_path_hashed'
g_BaseHost = 'https://en.wikipedia.org'
g_BasePage = '/'


g_ThreadsCount = 20

g_NeedTerminate = False;
g_Lock = threading.Lock()
g_LastRecvTime = time()
g_ProxyList = set()
g_LastProxyListTime = 0

g_HashAlphabetSize = 27
g_HashPolinom = None

g_DbConn = None

def print_err_msg() :
    print( "error: " + str( sys.exc_info() ) )

def get_nearest_next_prime( val ) :
    if ( val == 1 ) : return 2
    
    while ( True ) :
        prime_found = True
        for i in range( 2, val ) :
            if ( ( val % i ) == 0 ) :
                prime_found = False
                break

        if ( prime_found ) : return val
        val = val + 1
                
    
def calc_string_hash( s ) :
    global g_HashPolinom
    global g_HashAlphabetSize
    
    if ( g_HashPolinom == None ) :
        print( g_HashAlphabetSize )
        g_HashAlphabetSize = get_nearest_next_prime( g_HashAlphabetSize )
        print( g_HashAlphabetSize )
        g_HashPolinom = list()
        g_HashPolinom.append( 1 )

    hash = 0
    c_idx = -1
    for c in s :
        c_idx = c_idx + 1
        
        while ( c_idx >= len( g_HashPolinom ) ) :
            prior_ = g_HashPolinom[ len( g_HashPolinom ) - 1 ]
            next_ = prior_ * g_HashAlphabetSize
            next_ &= 0x7FFFFFFFFFFFFFFF
            g_HashPolinom.append( next_ )
            print( "next", next_ )

        hash += g_HashPolinom[ c_idx ] * ord( c )
        hash &= 0x7FFFFFFFFFFFFFFF

    return hash


def zipData( data ) :
    out = io.BytesIO()
    with gzip.GzipFile( fileobj = out, mode = "w" ) as f :
        f.write( data )
    return out.getvalue()

def runSqlCommit( sql, params = None ) :
    global g_DbConn
    with g_DbConn.cursor() as cursor :
        cursor.execute( sql, params )
    #g_DbConn.commit()

def updateLinkStatus( link_id, status ) :
    global g_LinksTblName
    runSqlCommit( "UPDATE " + g_LinksTblName + " SET process_status = %s, process_date = NOW() WHERE id = %s", ( str( status ), str( link_id ) ) )

def lockedUpdateLinkStatus( link_id, status ) :
    global g_Lock
    with g_Lock :
        updateLinkStatus( link_id, status )

def loadSqlResult( sql, params = None ) :
    global g_DbConn
    with g_DbConn.cursor() as cursor :
        cursor.execute( sql, params )
        return cursor.fetchone()

def appendPage( page ) :
    global g_LinksTblName
    sql = "SELECT COUNT( * ) as links_count FROM " + g_LinksTblName + " WHERE link_hashed = %s"
    if ( loadSqlResult( sql, ( calc_string_hash( page ) ) )[ "links_count" ] == 0 ) :
        runSqlCommit( "INSERT INTO " + g_LinksTblName + " ( link, create_date, link_hashed ) VALUES ( %s, NOW(), %s )", ( page, calc_string_hash( page ) ) )

def addLinks( host, page, links ) :
    global g_LinksPathTblName
    global g_Lock
    for link in links :
        if ( "href" not in link.attrs ) : continue

        href = link.attrs[ 'href' ]

        with g_Lock :
            appendPage( href )
            
            sql = "SELECT COUNT( * ) as links_count FROM " + g_LinksPathTblName + " WHERE link_from_hashed = %s AND link_to_hashed = %s"
            if ( loadSqlResult( sql, ( calc_string_hash( page ), calc_string_hash( href ) ) )[ "links_count" ] != 0 ) : continue
            
            runSqlCommit( "INSERT INTO " + g_LinksPathTblName + " ( link_from, link_to, create_date, link_from_hashed, link_to_hashed ) "
                          "VALUES ( %s, %s, NOW(), %s, %s )", ( page, href, calc_string_hash( page ), calc_string_hash( href ) ) )


def loadLinks( host, link_id, link ) :
    global g_Lock
    global g_LastRecvTime
    global g_LinksTblName
    
    try :
        url_page = host + link
        #print( url_page )

        html = None

        proxy_got = False;
        proxy_server = ""
        with g_Lock :
            if ( g_ProxyList != set() ) :
                rand_idx = random.randrange( 0, len( g_ProxyList ) )
                proxy_server = list( g_ProxyList )[ rand_idx ]
                #print( proxy_server )
                proxy_got = True

        request = Request( url_page, headers = { "User-Agent" : "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36" } )

        #proxy_got = False
        if ( not proxy_got ) :
            html = urlopen( request )
            #pass
        else :
            proxy_support = ProxyHandler( { "http" : "http://" + proxy_server } )
            opener = build_opener( proxy_support )
            html = opener.open( request )
            #print( type( html ) )
            #html = urlopen( request, proxies = { "http" : "http://" + proxy_server } )
            #print( proxy_server )
            
            
        try :
            html_file = html.read()
            html_gz = zipData( html_file )
            
            with g_Lock :
                updateLinkStatus( link_id, 2 )
                runSqlCommit( "UPDATE " + g_LinksTblName + " SET body_gz = %s WHERE id = %s",
                              ( html_gz, link_id ) )
                updateLinkStatus( link_id, 3 )
                
            bs = BeautifulSoup( html_file, "html.parser" )
            lockedUpdateLinkStatus( link_id, 4 )
            
            try :
                links = bs( "a", { "href" : re.compile( "^(/wiki/)([^:]*)$" ) } )
                try :
                    addLinks( host, link, links )
                except :
                    print_err_msg()
            except :
                print_err_msg()
                
            lockedUpdateLinkStatus( link_id, 5 )
            g_LastRecvTime = time()
        except :
            print_err_msg()
            lockedUpdateLinkStatus( link_id, 0 )
    except :
        print_err_msg()

def doRecvThread() :
    global g_NeedTerminate
    global g_Lock
    global g_BaseHost
    global g_LinksTblName
    
    #print( "recv_thread start" )
    while ( not g_NeedTerminate ) :
        new_page_id = -1
        new_page = ""
        new_page_found = False
        new_page_prepared = False
        
        with g_Lock :
            try :
                try :
                    row = loadSqlResult( "SELECT * FROM " + g_LinksTblName + " WHERE process_status = 0 ORDER BY create_date LIMIT 1" )
                    new_page = row[ 'link' ]
                    new_page_id = row[ 'id' ]
                    new_page_found = True
                except :
                    #print_err_msg()
                    pass
                    
                if ( new_page_found ) :
                    updateLinkStatus( new_page_id, 1 )
                    new_page_prepared = True
            except :
                print_err_msg()
                    
        if ( not new_page_prepared ) :
            sleep( 0.100 )
            continue
        
        loadLinks( g_BaseHost, new_page_id, new_page )
    #print( "recv_thread finish" )

def loadProxyList() :
    #print( "loading proxy list" )
    global g_Lock
    global g_ProxyList
    
    proxy_page = "http://www.prime-speed.ru/proxy/free-proxy-list/all-working-proxies.php"
    try :
        html = urlopen( proxy_page )
        html_file = html.read()
        bs = BeautifulSoup( html_file, "html.parser" )
        pre_tags = bs( "pre" )

        with g_Lock :
            g_ProxyList.clear()
            
            for pre_tag in pre_tags :
                rows = pre_tag.get_text( strip = True ).split( "\n" )
                for row in rows :
                    if ( len( row ) == 0 ) : continue

                    ip = re.compile( "^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}$" )
                    if ( not ip.match( row ) ) : continue

                    local_bind = "0.0.0.0"
                    if ( row[ 0 : len( local_bind ) ] == local_bind ) : continue

                    g_ProxyList.add( row )
    except :
        print_err_msg()

    
def doLinks( host, page ) :
    global g_Lock
    global g_ThreadsCount
    global g_LastProxyListTime
    global g_LastRecvTime
    global g_NeedTerminate
    global g_DbConn
    global g_MySqlHost
    global g_MySqlDb
    global g_MySqlUser
    global g_MySqlPwd
    global g_MySqlEncoding
    global g_MySqlConnTimeout
    global g_BaseHost
    global g_BasePage
    
    random.seed( time() )
    
    print( "do1" )
    with g_Lock :
        appendPage( page )
    
    print( "do2" )
    recv_threads = list()

    print( "do3" )
    with g_Lock :
        for t_i in range( 0, g_ThreadsCount ) :
            recv_th = threading.Thread( target = doRecvThread )
            recv_th.start()
            recv_threads.append( recv_th )

    #print( recv_threads )

    print( "do4" )
    try :
        while True :
            if ( ( g_LastProxyListTime + 5 * 60 ) < time() ) :
                loadProxyList()
                g_LastProxyListTime = time()
                
            if ( ( g_LastRecvTime + 2 * 60 ) < time() ) : break
            sleep( 0.1 )
    except :
        g_NeedTerminate = True
        print_err_msg()

    print( "do5" )
    for th in recv_threads :
        th.join()
    print( "do6" )
    
    
try :
    loadProxyList()
    g_LastProxyListTime = time()

    print( "1" )
    g_DbConn = pymysql.connect( host = g_MySqlHost,
                                db = g_MySqlDb,
                                user = g_MySqlUser,
                                password = g_MySqlPwd,
                                charset = g_MySqlEncoding,
                                connect_timeout = g_MySqlConnTimeout,
                                cursorclass = pymysql.cursors.DictCursor,
                                autocommit = True
                                )
    print( "2" )
    runSqlCommit( "UPDATE " + g_LinksTblName + " SET process_status = 0 WHERE process_status <> 5 AND process_status <> 0" )
    
    doLinks( g_BaseHost, g_BasePage )
    print( "3" )
except :
    print_err_msg()
finally :
    print( "4" )
    g_DbConn.close()
print( "all done" )

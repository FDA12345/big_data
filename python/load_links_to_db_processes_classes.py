from urllib.request import urlopen, Request, ProxyHandler, build_opener
from bs4 import BeautifulSoup, NavigableString
import re
import sys
from time import sleep, time
import pymysql.cursors
import io
import gzip
import random
from multiprocessing import Pool, Manager, Process, Lock, Value, Array

g_MySqlHost = '127.0.0.1'
g_MySqlDb = 'spider'
g_MySqlUser = 'root'
g_MySqlPwd = ''
g_MySqlEncoding = 'utf8'
g_MySqlConnTimeout = 5
g_LinksTblName = 'links_hashed'
g_LinksPathTblName = 'links_path_hashed'
g_BaseHost = 'https://en.wikipedia.org'
g_BasePage = '/'


g_ProcessesCount = 10

g_HashAlphabetSize = 27
g_HashPolinom = None

def print_err_msg() :
    print( "error: " + str( sys.exc_info() ) )
    sys.stdout.flush()


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
                
    
def calc_string_hash( hash_polinom, s ) :
    global g_HashAlphabetSize
    
    if ( hash_polinom == list() ) :
        hash_polinom.append( 1 )

    hash = 0
    c_idx = -1
    for c in s :
        c_idx = c_idx + 1
        
        while ( c_idx >= len( hash_polinom ) ) :
            prior_ = hash_polinom[ len( hash_polinom ) - 1 ]
            next_ = prior_ * g_HashAlphabetSize
            next_ &= 0x7FFFFFFFFFFFFFFF
            hash_polinom.append( next_ )
            #print( "next", next_ )

        hash += hash_polinom[ c_idx ] * ord( c )
        hash &= 0x7FFFFFFFFFFFFFFF

    return hash


def zipData( data ) :
    out = io.BytesIO()
    with gzip.GzipFile( fileobj = out, mode = "w" ) as f :
        f.write( data )
    return out.getvalue()

def runSqlCommit( db_conn, sql, params = None ) :
    with db_conn.cursor() as cursor :
        cursor.execute( sql, params )
	#db_conn.commit()

def updateLinkStatus( db_conn, link_id, status ) :
    global g_LinksTblName

    if ( status != 1 ) :
        runSqlCommit( db_conn, "UPDATE " + g_LinksTblName + " SET process_status = %s, process_date = NOW() WHERE id = %s", ( str( status ), str( link_id ) ) )
    else :
        runSqlCommit( db_conn, "UPDATE " + g_LinksTblName + " SET process_status = %s, process_date = NOW(), process_start_date = NOW() WHERE id = %s", ( str( status ), str( link_id ) ) )

def lockedUpdateLinkStatus( lock, db_conn, link_id, status ) :
    with lock :
        updateLinkStatus( db_conn, link_id, status )

def loadSqlResult( db_conn, sql, params = None ) :
    with db_conn.cursor() as cursor :
        cursor.execute( sql, params )
        return cursor.fetchone()

def loadSqlRowsCount( db_conn, sql, params = None ) :
    with db_conn.cursor() as cursor :
        cursor.execute( sql, params )
        try :
            return len( cursor.fetchall() )
        except :
            return 0
    

def appendPage( db_conn, page, hashed_page ) :
    global g_LinksTblName
    runSqlCommit( db_conn, "INSERT IGNORE INTO " + g_LinksTblName + " ( link, create_date, link_hashed ) VALUES ( %s, NOW(), %s )", ( page, hashed_page ) )


def findRandomProxy( lock, db_conn ) :
    try :
        with ( lock ) :
            min_max = loadSqlResult( db_conn, "SELECT MIN( id ) min_, MAX( id ) max_ FROM proxy_list" )
            min_id = min_max[ "min_" ]
            max_id = min_max[ "max_" ]
            rec_id = random.randrange( min_id, max_id + 1 )

            proxy_row = loadSqlResult( db_conn, "SELECT * FROM proxy_list WHERE id >= %s LIMIT 1", ( rec_id ) )
            return proxy_row[ "proxy" ]
    except :
        return None
    

def loadPageText( url_page, proxy ) :
    html = None

    request = Request( url_page, headers = { "User-Agent" : "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36" } )

    #proxy = None
    if ( proxy == None ) :
        html = urlopen( request )
    else :
        proxy_support = ProxyHandler( { "http" : "http://" + proxy } )
        opener = build_opener( proxy_support )
        html = opener.open( request )
            
    return html.read()


def updateProxyList( lock, conn_params ) :
    try :
        db_conn = openDbConn( conn_params )

        try :
            proxy_page = "http://www.prime-speed.ru/proxy/free-proxy-list/all-working-proxies.php"
            try :
                html = urlopen( proxy_page )
                html_file = html.read()
                bs = BeautifulSoup( html_file, "html.parser" )
                pre_tags = bs( "pre" )

                with lock :
                    runSqlCommit( db_conn, "TRUNCATE proxy_list" );
                    
                    for pre_tag in pre_tags :
                        rows = pre_tag.get_text( strip = True ).split( "\n" )
                        for row in rows :
                            if ( len( row ) == 0 ) : continue

                            ip = re.compile( "^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}$" )
                            if ( not ip.match( row ) ) : continue

                            local_bind = "0.0.0.0"
                            if ( row[ 0 : len( local_bind ) ] == local_bind ) : continue

                            runSqlCommit( db_conn, "INSERT INTO proxy_list ( proxy ) VALUES( %s )", ( row ) );
            except :
                print_err_msg()
        except :
            pass
        
        db_conn.close()
    except :
        pass

    print( "proxy list updated" )


def openDbConn( conn_params ) :
    conn = pymysql.connect( host = conn_params[ "host" ],
                                db = conn_params[ "db" ],
                                user = conn_params[ "user" ],
                                password = conn_params[ "pwd" ],
                                charset = conn_params[ "charset" ],
                                connect_timeout = conn_params[ "connect_timeout" ],
                                cursorclass = pymysql.cursors.DictCursor,
                                autocommit = True
                                )
    return conn


class UrlProcessorParams :
	def __init( self ) :
		pass
				
class UrlProcessor :
	def __init__( self, params ) :
		self.params = params
		
	def DoRecvProcess( self ) :
		self.hash_polinom = list();
		self.links_regex = re.compile( "^(/wiki/)([^:]*)$" )
		
		random.seed( time() )

		while ( not self.params.term_event.is_set() ) :
			try :
				self.db_conn = openDbConn( self.params.conn_params )
				self.DoRecvFunc()
			except :
				print_err_msg()
			finally :
				try :
					db_conn.close()
					sleep( 1.0 )
				except :
					pass

	def DoRecvFunc( self ) :
		global g_LinksTblName
		global g_BaseHost

		while ( not self.params.term_event.is_set() ) :
			new_page_id = -1
			new_page = ""
			new_page_found = False
			new_page_prepared = False
			
			with self.params.lock :
				row = loadSqlResult( self.db_conn, "SELECT * FROM " + g_LinksTblName + " WHERE process_status = 0 ORDER BY create_date LIMIT 1" )
				new_page = row[ 'link' ]
				new_page_id = row[ 'id' ]
				new_page_found = True
						
				if ( new_page_found ) :
					updateLinkStatus( self.db_conn, new_page_id, 1 )
					new_page_prepared = True
					print( new_page, new_page_id )

			if ( not new_page_prepared ) :
				sleep( 0.100 )
				continue

			LoadLinks( g_BaseHost, new_page_id, new_page )
			
	def LoadLinks( self, host, link_id, link ) :
		global g_LinksTblName
		
		proxy = findRandomProxy( self.params.lock, self.db_conn )
		url_page = host + link

		html_file = ""
		try :
			html_file = loadPageText( url_page, proxy )
		except KeyboardInterrupt :
			lockedUpdateLinkStatus( self.params.lock, self.db_conn, link_id, 0 )
			return
		except :
			#print_err_msg()
			lockedUpdateLinkStatus( self.params.lock, self.db_conn, link_id, 6 )
			return
		
		html_gz = zipData( html_file )
		runSqlCommit( self.db_conn, "UPDATE " + g_LinksTblName + " SET body_gz = %s WHERE id = %s", ( html_gz, link_id ) )

		try :
			bs = BeautifulSoup( html_file, "html.parser" )
			links = bs( "a", { "href" : self.links_regex } )
			lockedUpdateLinkStatus( self.lock, self.db_conn, link_id, 5 )
		except KeyboardInterrupt :
			lockedUpdateLinkStatus( self.lock, self.db_conn, link_id, 0 )
			return
		except :
			#print_err_msg()
			lockedUpdateLinkStatus( self.lock, self.db_conn, link_id, 7 )
			return

		AddLinks( host, link, links )
	
	def AddLinks( self,  host, page, links ) :
		global g_LinksPathTblName

		hashed_page = calc_string_hash( self.hash_polinom, page )

		insert_block_sql = list()
		insert_block_sql.append( "INSERT IGNORE INTO " + g_LinksPathTblName + " ( link_from, link_to, create_date, link_from_hashed, link_to_hashed ) VALUES " )
			
		for link in links :
			if ( "href" not in link.attrs ) : continue

			href = link.attrs[ 'href' ]
			hashed_href = calc_string_hash( self.hash_polinom, href )
				
			appendPage( self.db_conn, href, hashed_href )

			insert_block_sql.append( "( %s, %s, NOW(), %d, %d )" % ( self.db_conn.escape( page ), self.db_conn.escape( href ), hashed_page, hashed_href ) )
			insert_block_sql.append( ", " )
				
		if ( len( insert_block_sql ) == 1 ) : return

		del insert_block_sql[ -1 ]
		insert_block_sql.append( ";" )

		sql = "".join( insert_block_sql )
		runSqlCommit( self.db_conn, sql )
	

def statisDoRecvProcess( term_event, lock, conn_params ) :
	params = UrlProcessorParams()
	params.term_event = term_event
	params.lock = lock
	params.conn_params = conn_params
	
	processor = UrlProcessor( params )
	processor.DoRecvProcess()
					
					
if __name__ == '__main__' :
    g_HashAlphabetSize = get_nearest_next_prime( g_HashAlphabetSize )
    
    with Manager() as mgr :
		params = UrlProcessorParams()
	
        params.term_event = mgr.Event()
        params.lock = mgr.Lock()
        
        params.conn_params = mgr.dict()
        params.conn_params[ "host" ] = g_MySqlHost
        params.conn_params[ "db" ] = g_MySqlDb
        params.conn_params[ "user" ] = g_MySqlUser
        params.conn_params[ "pwd" ] = g_MySqlPwd
        params.conn_params[ "charset" ] = g_MySqlEncoding
        params.conn_params[ "connect_timeout" ] = g_MySqlConnTimeout

        db_conn = openDbConn( conn_params )
        hash_polinom = list()
        appendPage( db_conn, g_BasePage, calc_string_hash( hash_polinom, g_BasePage ) )
        runSqlCommit( db_conn, "UPDATE links_hashed SET process_status = 0, process_start_date = NULL WHERE process_status <> 5 AND process_status <> 0" )
        db_conn.close()

        updateProxyList( lock, conn_params )
        proxy_list_last_time = time()

        processes = list()
        for p_i in range( 0, g_ProcessesCount ) :
            p = Process( target = statisDoRecvProcess, args = ( term_event, lock, conn_params ) )
            processes.append( p )
            p.start()

        print( "press Ctrl+C for terminate" )

        try :
            while True :
                if ( ( proxy_list_last_time + 5 * 60 ) < time() ) :
                    updateProxyList( lock, conn_params )
                    proxy_list_last_time = time()
                    
                #if ( ( g_LastRecvTime + 2 * 60 ) < time() ) : break
                sleep( 0.1 )
        except :
            print_err_msg()
        finally :
            term_event.set()            

        print( "term event signaled" )
        for p in processes :
            p.join()

        print( "all done" )


    

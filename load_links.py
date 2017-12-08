from urllib.request import urlopen
from bs4 import BeautifulSoup, NavigableString
import re
import sys
import threading
from time import sleep, time

g_BaseHost = "http://en.wikipedia.org"
g_BasePage = "/"


g_ThreadsCount = 10
g_OutFileName = "links_en_lvl_" + str( g_ThreadsCount ) + "_threads.txt"

g_NeedTerminate = False;
g_Lock = threading.Lock()
g_NewPages = set()
g_ProcessedPages = set()
g_LastRecvTime = time()

def print_err_msg() :
    print( "error: " + str( sys.exc_info() ) )

def addLinks( host, page, links ) :
    for link in links :
        if ( "href" not in link.attrs ) : continue

        href = link.attrs[ 'href' ]

        with g_Lock :
            if ( href in g_ProcessedPages ) : continue
            if ( href in g_NewPages ) : continue
            
            g_NewPages.add( href )

            with open( g_OutFileName, "a+" ) as f :
                f.write( "page " + page + " link " + str( href ) + "\n" )
                f.flush()

            g_LastRecvTime = time()


def loadLinks( host, page ) :
    try :
        html = urlopen( host + page )
        try :
            html_file = html.read()
            #try :
            #    with open( "pages/" + page + ".html", "wb" ) as f :
            #        f.write( html_file )
            #        f.close()
            #except :
            #    pass
                
            bs = BeautifulSoup( html_file, "html.parser" )
            try :
                links = bs( "a", { "href" : re.compile( "^(/wiki/)([^:]*)$" ) } )
                try :
                    addLinks( host, page, links )
                except :
                    print_err_msg()
            except :
                print_err_msg()
        except :
            print_err_msg()
    except :
        print_err_msg()

def doRecvThread() :
    while ( not g_NeedTerminate ) :
        new_page = ""
        
        with g_Lock :
            if ( g_NewPages != set() ) :
                new_page = g_NewPages.pop()
                g_ProcessedPages.add( new_page )
                
        if ( new_page == "" ) :
            sleep( 0.010 )
            continue
        
        loadLinks( g_BaseHost, new_page )
    
def doLinks( host, page ) :
    g_NewPages.add( page )
    #goLinks( host )
    recv_threads = list()

    with g_Lock :
        for t_i in range( 1, g_ThreadsCount ) :
            recv_th = threading.Thread( target = doRecvThread )
            recv_th.start()
            recv_threads.append( recv_th )

    try :
        while True :
            if ( g_LastRecvTime > ( time() + 2 * 60 ) ) : break
            sleep( 0.1 )
    except :
        g_NeedTerminate = True

    for th in recv_threads :
        th.join()
    

doLinks( g_BaseHost, g_BasePage )
print( "all done" )

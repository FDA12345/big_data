from urllib.request import urlopen
from bs4 import BeautifulSoup, NavigableString
import re
import sys

g_NewPages = set()
g_ProcessedPages = set()

def print_err_msg() :
    print( "error: " + str( sys.exc_info() ) )

def addLinks( host, page, links ) :
    for link in links :
        if ( "href" not in link.attrs ) : continue

        href = link.attrs[ 'href' ]

        if ( href in g_ProcessedPages ) : continue
        if ( href in g_NewPages ) : continue
        
        g_NewPages.add( href )

        with open( "links_en_lvl_100_new.txt", "a+" ) as f :
            f.write( "page " + page + " link " + str( href ) + "\n" )
            f.flush()


def loadLinks( host, page ) :
    try :
        html = urlopen( host + page )
        try :
            bs = BeautifulSoup( html.read(), "html.parser" )
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

def goLinks( host ) :
    while ( g_NewPages != set() ) :
        page = g_NewPages.pop()
        g_ProcessedPages.add( page )
        loadLinks( host, page )

def doLinks( host, page ) :
    g_NewPages.add( page )
    goLinks( host )
    

base_host = "http://en.wikipedia.org"
base_page = "/"

doLinks( base_host, base_page )
print( "all done" )

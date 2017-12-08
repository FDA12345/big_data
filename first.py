from urllib.request import urlopen
from bs4 import BeautifulSoup, NavigableString

html = urlopen( "http://www.pythonscraping.com/pages/page3.html" )
bs = BeautifulSoup( html.read(), "html.parser" )

rows = bs.find( "table", { "id" : "giftList" } ).findAll( "tr" )
for row in rows :
    #if isinstance( row, NavigableString ) : continue
    #print( type( row ) )

    cells = row( "td" )
    if ( len( cells ) == 0 ) : continue

    print( cells[ 0 ].get_text( strip = True ), "->",
           cells[ 2 ].get_text( strip = True ) )

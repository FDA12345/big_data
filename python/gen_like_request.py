from time import sleep

g_MaxLinkSize = 50
g_StrPattern = '#'

g_HashAlphabetSize = 27
g_HashPolinom = list()

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


pattern_len = len( g_StrPattern )
mask_count = g_MaxLinkSize - pattern_len
print( "mask_count", mask_count )
sql = ''
head_str = ''
for i in range( 0, mask_count + 1 ) :
    head_hash = calc_string_hash( g_HashPolinom, head_str )
    total_hash = calc_string_hash( g_HashPolinom, head_str + g_StrPattern )
    pattern_hash = ( total_hash - head_hash )
    pattern_hash &= 0x7FFFFFFFFFFFFFFF
    print ( pattern_hash )
    head_str = head_str + '*'
    sql = sql + 'SELECT * FROM links_hashed WHERE link_hashed & ' + str( pattern_hash ) + ' = ' + str( pattern_hash )
    if ( i < mask_count ) :
        sql += '\nUNION ALL\n'

print( "done, sql", sql )

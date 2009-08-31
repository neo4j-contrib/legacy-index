/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.util.index;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.util.ArrayMap;

public class LuceneReadOnlyDataSource // extends XaDataSource
{
    private final ArrayMap<String,IndexSearcher> indexSearchers = 
        new ArrayMap<String,IndexSearcher>( 6, true, true );

    private final String storeDir;
    
    private Map<String,LruCache<String,Iterable<Long>>> caching = 
        Collections.synchronizedMap( 
            new HashMap<String,LruCache<String,Iterable<Long>>>() );

    public LuceneReadOnlyDataSource( String directory ) 
    {
        this.storeDir = directory;
        String dir = storeDir;
        File file = new File( dir );
        if ( !file.exists() )
        {
            throw new RuntimeException( "No such directory " + dir );
        }
    }
    
    public void close()
    {
        for ( IndexSearcher searcher : indexSearchers.values() )
        {
            try
            {
                searcher.close();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        indexSearchers.clear();
    }

    IndexSearcher getIndexSearcher( String key )
    {
        IndexSearcher searcher = indexSearchers.get( key );
        if ( searcher == null )
        {
            try
            {
                Directory dir = FSDirectory.getDirectory( 
                    new File( storeDir + "/" + key ) );
                if ( dir.list().length == 0 )
                {
                    return null;
                }
                searcher = new IndexSearcher( dir );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            indexSearchers.put( key, searcher );
        }
        return searcher;
    }

    public LruCache<String,Iterable<Long>> getFromCache( String key )
    {
        return caching.get( key );
    }

    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        this.caching.put( key, new LruCache<String,Iterable<Long>>( key,
            maxNumberOfCachedEntries, null ) );
    }

    void invalidateCache( String key, Object value )
    {
        LruCache<String,Iterable<Long>> cache = caching.get( key );
        if ( cache != null )
        {
            cache.remove( value.toString() );
        }
    }
}

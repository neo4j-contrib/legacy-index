/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

package org.neo4j.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.util.ArrayMap;

/**
 * The underlying XA data source for a {@link LuceneReadOnlyIndexService}. This
 * class is public since the XA framework requires it.
 */
public class LuceneReadOnlyDataSource // extends XaDataSource
{
    private final ArrayMap<String, IndexSearcher> indexSearchers = new ArrayMap<String, IndexSearcher>(
            6, true, true );

    private final String storeDir;

    private Map<String, LruCache<String, Collection<Long>>> caching = Collections.synchronizedMap( new HashMap<String, LruCache<String, Collection<Long>>>() );

    /**
     * @param directory the root directory where the Lucene indexes reside.
     */
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

    /**
     * Closes this index service and frees all resources.
     */
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
                File fsDirectory = new File( storeDir, key );
                if ( !fsDirectory.exists() )
                {
                    return null;
                }
                Directory dir = FSDirectory.open( fsDirectory );
                if ( dir.listAll().length == 0 )
                {
                    return null;
                }
                searcher = new IndexSearcher( dir, true );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            indexSearchers.put( key, searcher );
        }
        return searcher;
    }

    LruCache<String, Collection<Long>> getFromCache( String key )
    {
        return caching.get( key );
    }

    void enableCache( String key, int maxNumberOfCachedEntries )
    {
        this.caching.put( key, new LruCache<String, Collection<Long>>( key,
                maxNumberOfCachedEntries, null ) );
    }

    void invalidateCache( String key, Object value )
    {
        LruCache<String, Collection<Long>> cache = caching.get( key );
        if ( cache != null )
        {
            cache.remove( value.toString() );
        }
    }

    void invalidateCache()
    {
        caching.clear();
    }
}

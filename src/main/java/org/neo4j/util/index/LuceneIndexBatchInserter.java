/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
import java.io.Reader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.impl.batchinsert.BatchInserter;
import org.neo4j.impl.util.ArrayMap;

public class LuceneIndexBatchInserter
{
    private final String storeDir;

    private final ArrayMap<String,IndexWriter> indexWriters = 
        new ArrayMap<String,IndexWriter>( 6, false, false );

    private final Analyzer fieldAnalyzer = new Analyzer()
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
        }
    };
    
    public LuceneIndexBatchInserter( BatchInserter neo )
    {
        this.storeDir = fixPath( neo.getStore() + "/lucene" );
    }
    
    private String fixPath( String dir )
    {
        String store = dir;
        String fileSeparator = System.getProperty( "file.separator" );
        if ( "\\".equals( fileSeparator ) )
        {
            store = dir.replace( '/', '\\' );
        }
        else if ( "/".equals( fileSeparator ) )
        {
            store = dir.replace( '\\', '/' );
        }
        File directories = new File( dir );
        if ( !directories.exists() )
        {
            if ( !directories.mkdirs() )
            {
                throw new RuntimeException( "Unable to create directory path["
                    + storeDir + "] for Lucene index store." );
            }
        }
        return store;
    }
    
    public void index( long node, String key, Object value )
    {
        IndexWriter writer = indexWriters.get( key );
        if ( writer == null )
        {
            try
            {
                Directory dir = FSDirectory.getDirectory( 
                    new File( storeDir + "/" + key ) );
                writer = new IndexWriter( dir, fieldAnalyzer,
                    MaxFieldLength.UNLIMITED );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            indexWriters.put( key, writer );
        }
        Document document = new Document();
        document.add( new Field( LuceneIndexService.DOC_ID_KEY,
            String.valueOf( node ), Field.Store.YES,
            Field.Index.NOT_ANALYZED ) );
        document.add( new Field( LuceneIndexService.DOC_INDEX_KEY,
            value.toString(), Field.Store.NO, Field.Index.NOT_ANALYZED ) );
        try
        {
            writer.addDocument( document );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        if ( key.equals( cachedForKey ) )
        {
            cachedIndexSearcher = null;
        }
    }
    
    public void close()
    {
        for ( IndexWriter writer : indexWriters.values() )
        {
            try
            {
                writer.close();
            }
            catch ( CorruptIndexException e )
            {
                throw new RuntimeException( e );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private IndexSearcher cachedIndexSearcher = null;
    private String cachedForKey = null;
    
    Iterable<Long> getNodes( String key, Object value )
    {
        IndexWriter writer = indexWriters.remove( key );
        if ( writer != null )
        {
            try
            {
                writer.close();
            }
            catch ( CorruptIndexException e )
            {
                throw new RuntimeException( e );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        Set<Long> nodeSet = new HashSet<Long>();
        if ( !key.equals( cachedForKey ) || cachedIndexSearcher == null )
        {
            try
            {
                Directory dir = FSDirectory.getDirectory( 
                    new File( storeDir + "/" + key ) );
                cachedForKey = key;
                if ( dir.list().length == 0 )
                {
                    return Collections.EMPTY_SET;
                }
                cachedIndexSearcher = new IndexSearcher( dir );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        Query query = new TermQuery( new Term( LuceneIndexService.DOC_INDEX_KEY, 
            value.toString() ) );
        try
        {
            Hits hits = cachedIndexSearcher.search( query );
            for ( int i = 0; i < hits.length(); i++ )
            {
                Document document = hits.doc( i );
                long id = Long.parseLong( document.getField(
                    LuceneIndexService.DOC_ID_KEY ).stringValue() );
                nodeSet.add( id );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return nodeSet;
    }
}
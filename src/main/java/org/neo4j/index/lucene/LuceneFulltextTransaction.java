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
package org.neo4j.index.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

class LuceneFulltextTransaction extends LuceneTransaction
{
    private final Map<String, DirectoryAndWorkers> fulltextIndexed =
        new HashMap<String, DirectoryAndWorkers>();
    private final Map<String, DirectoryAndWorkers> fulltextRemoved =
        new HashMap<String, DirectoryAndWorkers>();
    
    private static final String DOC_ALWAYS_HIT = "always_hit";
    private static final String ALWAYS_HIT_VALUE = "1";
    
    LuceneFulltextTransaction( int identifier, XaLogicalLog xaLog,
        LuceneDataSource luceneDs )
    {
        super( identifier, xaLog, luceneDs );
    }
    
    private DirectoryAndWorkers getDirectory(
        Map<String, DirectoryAndWorkers> map, String key )
    {
        DirectoryAndWorkers result = map.get( key );
        if ( result == null )
        {
            Directory directory = new RAMDirectory();
            try
            {
                IndexWriter writer = new IndexWriter( directory,
                    getDataSource().getAnalyzer(), true,
                    MaxFieldLength.UNLIMITED );
                writer.close();
                result = new DirectoryAndWorkers( directory );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            map.put( key, result );
        }
        return result;
    }
    
    private IndexWriter newIndexWriter( Directory directory )
        throws IOException
    {
        return new IndexWriter( directory,
            getDataSource().getAnalyzer(),
            MaxFieldLength.UNLIMITED );
    }
    
    private void insertAndRemove( DirectoryAndWorkers insertTo,
        DirectoryAndWorkers removeFrom, Node node, String key, Object value )
    {
        try
        {
            if ( node == null && value == null )
            {
                removeFrom.writer.deleteAll();
            }
            else
            {
                BooleanQuery deletionQuery = new BooleanQuery();
                if ( value != null )
                {
                    deletionQuery.add( new TermQuery( new Term( getDataSource().
                        getDeleteDocumentsKey(), value.toString() ) ),
                        Occur.MUST );
                }
                deletionQuery.add( new TermQuery( new Term(
                    LuceneIndexService.DOC_ID_KEY, "" + node.getId() ) ),
                    Occur.MUST );
                removeFrom.writer.deleteDocuments( deletionQuery );
            }
            removeFrom.invalidateSearcher();
            
            if ( node == null && value == null )
            {
                insertTo.all = true;
            }
            else
            {
                Document document = new Document();
                this.getDataSource().fillDocument( document, node.getId(), key,
                    value != null ? value : "" );
                if ( value == null )
                {
                    document.add( new Field( DOC_ALWAYS_HIT, ALWAYS_HIT_VALUE,
                        Store.NO, Index.NOT_ANALYZED ) );
                }
                insertTo.writer.addDocument( document );
            }
            insertTo.invalidateSearcher();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    void index( Node node, String key, Object value )
    {
        super.index( node, key, value );
        insertAndRemove( getDirectory( fulltextIndexed, key ),
            getDirectory( fulltextRemoved, key ), node, key, value );
    }
    
    @Override
    boolean hasModifications( String key )
    {
        return fulltextIndexed.containsKey( key ) || fulltextRemoved.containsKey( key );
    }
    
    @Override
    boolean getIndexDeleted( String key )
    {
        return getDirectory( fulltextRemoved, key ).all;
    }

    @Override
    void removeIndex( Node node, String key, Object value )
    {
        super.removeIndex( node, key, value );
        insertAndRemove( getDirectory( fulltextRemoved, key ),
            getDirectory( fulltextIndexed, key ), node, key, value );
    }
    
    @Override
    Set<Long> getDeletedNodesFor( String key, Object value, Object matching )
    {
        return getNodes( getDirectory( fulltextRemoved, key ), key, value, matching,
            true );
    }

    @Override
    Set<Long> getNodesFor( String key, Object value, Object matching )
    {
        return getNodes( getDirectory( fulltextIndexed, key ), key, value, matching,
            false );
    }
    
    private Set<Long> getNodes( DirectoryAndWorkers directory, String key,
        Object value, Object matching, boolean includeAlwaysHit )
    {
        try
        {
            IndexSearcher searcher = directory.getSearcher();
            Query query =
                getDataSource().getIndexService().formQuery( key, value, matching );
            if ( includeAlwaysHit )
            {
                BooleanQuery booleanQuery = new BooleanQuery();
                booleanQuery.add( query, Occur.SHOULD );
                booleanQuery.add( new TermQuery(
                    new Term( DOC_ALWAYS_HIT, ALWAYS_HIT_VALUE ) ),
                    Occur.SHOULD );
                query = booleanQuery;
            }
            Hits hits = searcher.search( query );
            HashSet<Long> result = new HashSet<Long>();
            for ( int i = 0; i < hits.length(); i++ )
            {
                result.add( Long.parseLong( hits.doc( i ).getField(
                    LuceneIndexService.DOC_ID_KEY ).stringValue() ) );
            }
            return result;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Override
    protected void doCommit()
    {
        for ( DirectoryAndWorkers directory :
            this.fulltextIndexed.values() )
        {
            directory.close();
        }
        for ( DirectoryAndWorkers directory :
            this.fulltextRemoved.values() )
        {
            directory.close();
        }
        super.doCommit();
    }

    private class DirectoryAndWorkers
    {
        private final Directory directory;
        private final IndexWriter writer;
        private IndexSearcher searcher;
        private boolean all;
        
        private DirectoryAndWorkers( Directory directory )
            throws IOException
        {
            this.directory = directory;
            this.writer = newIndexWriter( directory );
        }
        
        private void safeClose( Object object )
        {
            if ( object == null )
            {
                return;
            }
            
            try
            {
                if ( object instanceof IndexWriter )
                {
                    ( ( IndexWriter ) object ).close();
                }
                else if ( object instanceof IndexSearcher )
                {
                    ( ( IndexSearcher ) object ).close();
                }
            }
            catch ( IOException e )
            {
                // Ok
            }
        }
        
        private void invalidateSearcher()
        {
            safeClose( this.searcher );
            this.searcher = null;
        }
        
        private void close()
        {
            safeClose( this.writer );
            invalidateSearcher();
        }
        
        private IndexSearcher getSearcher()
        {
            try
            {
                if ( this.searcher == null )
                {
                    this.writer.commit();
                    this.searcher = new IndexSearcher( directory, true );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            return this.searcher;
        }
    }
}

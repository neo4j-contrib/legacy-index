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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.transaction.xaframework.XaCommand;
import org.neo4j.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaContainer;
import org.neo4j.impl.transaction.xaframework.XaDataSource;
import org.neo4j.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.impl.transaction.xaframework.XaTransaction;
import org.neo4j.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.impl.util.ArrayMap;

public class LuceneDataSource extends XaDataSource
{
    private final ArrayMap<String,IndexSearcher> indexSearchers = 
        new ArrayMap<String,IndexSearcher>( 6, true, true );

    private final XaContainer xaContainer;
    private final String storeDir;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); 
    private final Analyzer fieldAnalyzer;
    private final LuceneIndexStore store;
    private LuceneIndexService indexService;
    
    private Map<String,LruCache<String,Collection<Long>>> caching = 
        Collections.synchronizedMap( 
            new HashMap<String,LruCache<String,Collection<Long>>>() );

    public LuceneDataSource( Map<Object,Object> params ) 
        throws InstantiationException
    {
        super( params );
        this.storeDir = (String) params.get( "dir" );
        this.fieldAnalyzer = instantiateAnalyzer();
        String dir = storeDir;
        File file = new File( dir );
        if ( !file.exists() )
        {
            try
            {
                autoCreatePath( dir );
            }
            catch ( IOException e )
            {
                throw new RuntimeException(
                    "Unable to create directory " + dir, e );
            }
        }
        this.store = new LuceneIndexStore( storeDir + "/lucene-store.db" );
        XaCommandFactory cf = new LuceneCommandFactory();
        XaTransactionFactory tf = new LuceneTransactionFactory( store );
        xaContainer = XaContainer.create( dir + "/lucene.log", cf, tf, params );
        try
        {
            xaContainer.openLogicalLog();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to open lucene log in " + dir,
                e );
        }
    }
    
    /**
     * This is here so that {@link LuceneIndexService#formQuery(String, Object)}
     * can be used when getting stuff from inside a transaction.
     * @param indexService the {@link LuceneIndexService} instance which
     * created it.
     */
    protected void setIndexService( LuceneIndexService indexService )
    {
        this.indexService = indexService;
    }
    
    protected LuceneIndexService getIndexService()
    {
        return this.indexService;
    }

    private Analyzer instantiateAnalyzer()
    {
        return new Analyzer()
        {
            @Override
            public TokenStream tokenStream( String fieldName, Reader reader )
            {
                return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
            }
        };
    }

    private void autoCreatePath( String dirs ) throws IOException
    {
        File directories = new File( dirs );
        if ( !directories.exists() )
        {
            if ( !directories.mkdirs() )
            {
                throw new IOException( "Unable to create directory path["
                    + dirs + "] for Neo store." );
            }
        }
    }

    @Override
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
        xaContainer.close();
        store.close();
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new LuceneXaConnection( storeDir, xaContainer
            .getResourceManager(), getBranchId() );
    }
    
    protected Analyzer getAnalyzer()
    {
        return this.fieldAnalyzer;
    }
    
    private class LuceneCommandFactory extends XaCommandFactory
    {
        LuceneCommandFactory()
        {
            super();
        }

        @Override
        public XaCommand readCommand( ReadableByteChannel channel, 
            ByteBuffer buffer ) throws IOException
        {
            return LuceneCommand.readCommand( channel, buffer );
        }
    }
    
    private class LuceneTransactionFactory extends XaTransactionFactory
    {
        private final LuceneIndexStore store;
        
        LuceneTransactionFactory( LuceneIndexStore store )
        {
            this.store = store;
        }
        
        @Override
        public XaTransaction create( int identifier )
        {
            return createTransaction( identifier, this.getLogicalLog() );
        }

        @Override
        public void flushAll()
        {
            // Not much we can do...
        }

        @Override
        public long getCurrentVersion()
        {
            return store.getVersion();
        }
        
        @Override
        public long getAndSetNewVersion()
        {
            return store.incrementVersion();
        }
    }
    
    public void getReadLock()
    {
        lock.readLock().lock();
    }
    
    public void releaseReadLock()
    {
        lock.readLock().unlock();
    }
    
    public void getWriteLock()
    {
        lock.writeLock().lock();
    }
    
    public void releaseWriteLock()
    {
        lock.writeLock().unlock();
    }
    
    /**
     * If nothing has changed underneath (since the searcher was last created
     * or refreshed) {@code null} is returned. But if something has changed a
     * refreshed searcher is returned. It makes use if the
     * {@link IndexReader#reopen()} which faster than opening an index from
     * scratch.
     * 
     * @param searcher the {@link IndexSearcher} to refresh.
     * @return a refreshed version of the searcher or, if nothing has changed,
     * {@code null}.
     * @throws IOException if there's a problem with the index.
     */
    private IndexSearcher refreshSearcher( IndexSearcher searcher )
    {
        try
        {
            IndexReader reopened = searcher.getIndexReader().reopen();
            if ( reopened != null )
            {
                return new IndexSearcher( reopened );
            }
            return null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private Directory getDirectory( String key ) throws IOException
    {
        return FSDirectory.open( new File( storeDir, key ) );
    }
    
    /**
     * @param key the key for the index, i.e. which index to return a searcher
     * for
     * @return an {@link IndexSearcher} for the index for {@key}. If no such
     * searcher has been opened before it is opened here.
     */
    IndexSearcher getIndexSearcher( String key )
    {
        try
        {
            IndexSearcher searcher = indexSearchers.get( key );
            if ( searcher == null )
            {
                Directory dir = getDirectory( key );
                try
                {
                    if ( dir.listAll().length == 0 )
                    {
                        return null;
                    }
                }
                catch ( IOException e )
                {
                    return null;
                }
                searcher = new IndexSearcher( dir, false );
                indexSearchers.put( key, searcher );
            }
            return searcher;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog )
    {
        return new LuceneTransaction( identifier, logicalLog, this );
    }

    void invalidateIndexSearcher( String key )
    {
        IndexSearcher searcher = indexSearchers.get( key );
        if ( searcher != null )
        {
            IndexSearcher refreshedSearcher = refreshSearcher( searcher );
            if ( refreshedSearcher != null )
            {
                indexSearchers.put( key, refreshedSearcher );
            }
        }
    }

    synchronized IndexWriter getIndexWriter( String key )
    {
        try
        {
            Directory dir = getDirectory( key );
            return new IndexWriter( dir, getAnalyzer(),
                MaxFieldLength.UNLIMITED );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    protected void deleteDocumentsUsingWriter( IndexWriter writer,
        long nodeId, Object value )
    {
        try
        {
            BooleanQuery query = new BooleanQuery();
            query.add( new TermQuery( new Term( getDeleteDocumentsKey(),
                value.toString() ) ), Occur.MUST );
            query.add( new TermQuery( new Term( LuceneIndexService.DOC_ID_KEY,
                "" + nodeId ) ), Occur.MUST );
            writer.deleteDocuments( query );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to delete for " + nodeId + ","
                + "," + value + " using" + writer, e );
        }
    }
    
    protected String getDeleteDocumentsKey()
    {
        return LuceneIndexService.DOC_INDEX_KEY;
    }

    void removeWriter( String key, IndexWriter writer )
    {
        try
        {
            writer.close();
        }
        catch ( CorruptIndexException e )
        {
            throw new RuntimeException( "Unable to close lucene writer "
                + writer, e );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to close lucene writer "
                + writer, e );
        }
    }

    public LruCache<String,Collection<Long>> getFromCache( String key )
    {
        return caching.get( key );
    }

    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        this.caching.put( key, new LruCache<String,Collection<Long>>( key,
            maxNumberOfCachedEntries, null ) );
    }

    void invalidateCache( String key, Object value )
    {
        LruCache<String,Collection<Long>> cache = caching.get( key );
        if ( cache != null )
        {
            cache.remove( value.toString() );
        }
    }

    protected void fillDocument( Document document, long nodeId, String key,
        Object value )
    {
        document.add( new Field( LuceneIndexService.DOC_ID_KEY,
            String.valueOf( nodeId ), Field.Store.YES,
            Field.Index.NOT_ANALYZED ) );
        document.add( new Field( LuceneIndexService.DOC_INDEX_KEY,
            value.toString(), Field.Store.NO,
            getIndexStrategy( key, value ) ) );
    }

    protected Index getIndexStrategy( String key, Object value )
    {
        return Field.Index.NOT_ANALYZED;
    }

    @Override
    public void keepLogicalLogs( boolean keep )
    {
        xaContainer.getLogicalLog().setKeepLogs( keep );
    }
    
    @Override
    public long getCreationTime()
    {
        return store.getCreationTime();
    }
    
    @Override
    public long getRandomIdentifier()
    {
        return store.getRandomNumber();
    }
    
    @Override
    public long getCurrentLogVersion()
    {
        return store.getVersion();
    }
    
    public long incrementAndGetLogVersion()
    {
        return store.incrementVersion();
    }
    
    public void setCurrentLogVersion( long version )
    {
        store.setVersion( version );
    }
    
    @Override
    public void applyLog( ReadableByteChannel byteChannel ) throws IOException
    {
        xaContainer.getLogicalLog().applyLog( byteChannel );
    }
    
    @Override
    public void rotateLogicalLog() throws IOException
    {
        // flush done inside rotate
        xaContainer.getLogicalLog().rotate();
    }
    
    @Override
    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        return xaContainer.getLogicalLog().getLogicalLog( version );
    }
    
    @Override
    public boolean hasLogicalLog( long version )
    {
        return xaContainer.getLogicalLog().hasLogicalLog( version );
    }
    
    @Override
    public boolean deleteLogicalLog( long version )
    {
        return xaContainer.getLogicalLog().deleteLogicalLog( version );
    }
    
    @Override
    public void setAutoRotate( boolean rotate )
    {
        xaContainer.getLogicalLog().setAutoRotateLogs( rotate );
    }
    
    @Override
    public void setLogicalLogTargetSize( long size )
    {
        xaContainer.getLogicalLog().setLogicalLogTargetSize( size );
    }
    
    @Override
    public void makeBackupSlave()
    {
        xaContainer.getLogicalLog().makeBackupSlave();
    }
}

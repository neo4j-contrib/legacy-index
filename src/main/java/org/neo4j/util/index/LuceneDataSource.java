package org.neo4j.util.index;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
    // private final LockManager lockManager;
    private final int LOCK_STRIPE_SIZE = 5;
    private ReentrantReadWriteLock[] keyLocks = 
        new ReentrantReadWriteLock[LOCK_STRIPE_SIZE];
    private final Analyzer fieldAnalyzer;
    private final LuceneIndexStore store;
    
    private Map<String,LruCache<String,Iterable<Long>>> caching = 
        Collections.synchronizedMap( 
            new HashMap<String,LruCache<String,Iterable<Long>>>() );

    public LuceneDataSource( Map<?,?> params ) throws InstantiationException
    {
        super( params );
        // this.lockManager = (LockManager) params.get( LockManager.class );
        for ( int i = 0; i < keyLocks.length; i++ )
        {
            keyLocks[i] = new ReentrantReadWriteLock();
        }
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
        xaContainer = XaContainer.create( dir + "/lucene.log", cf, tf );
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
    
    private void getReadLock( String key )
    {
        keyLocks[ Math.abs( key.hashCode() ) % 
            LOCK_STRIPE_SIZE ].readLock().lock();
    }
    
    private void releaseReadLock( String key )
    {
        keyLocks[ Math.abs( key.hashCode() ) % 
            LOCK_STRIPE_SIZE ].readLock().unlock();
    }

    private void getWriteLock( String key )
    {
        keyLocks[ Math.abs( key.hashCode() ) % 
            LOCK_STRIPE_SIZE ].writeLock().lock();
    }
    
    private void releaseWriteLock( String key )
    {
        keyLocks[ Math.abs( key.hashCode() ) % 
            LOCK_STRIPE_SIZE ].writeLock().unlock();
    }
    
    IndexSearcher acquireIndexSearcher( String key )
    {
        getReadLock( key );
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

    public XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog )
    {
        return new LuceneTransaction( identifier, logicalLog, this );
    }

    void releaseIndexSearcher( String key, IndexSearcher searcher )
    {
        releaseReadLock( key );
    }

    void removeIndexSearcher( String key )
    {
        getWriteLock( key );
        try
        {
            IndexSearcher searcher = indexSearchers.remove( key );
            if ( searcher != null )
            {
                try
                {
                    searcher.close();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException(
                        "Unable to close index searcher[" + key + "]", e );
                }
            }
        }
        finally
        {
            releaseWriteLock( key );
        }
    }

    synchronized IndexWriter getIndexWriter( String key )
    {
        try
        {
            getWriteLock( key );
            Directory dir = FSDirectory.getDirectory( 
                new File( storeDir + "/" + key ) );
            return new IndexWriter( dir, getAnalyzer(),
                MaxFieldLength.UNLIMITED );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected void deleteDocumentUsingReader( IndexSearcher searcher,
        long nodeId, Object value )
    {
        if ( searcher == null )
        {
            return;
        }
        Query query = new TermQuery( new Term( getDeleteDocumentsKey(),
            value.toString() ) );
        try
        {
            Hits hits = searcher.search( query );
            for ( int i = 0; i < hits.length(); i++ )
            {
                Document document = hits.doc( i );
                int foundId = Integer.parseInt( document.getField(
                    LuceneIndexService.DOC_ID_KEY ).stringValue() );
                if ( nodeId == foundId )
                {
                    int docNum = hits.id( i );
                    searcher.getIndexReader().deleteDocument( docNum );
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to delete for " + nodeId + ","
                + "," + value + " using" + searcher, e );
        }
    }
    
    protected String getDeleteDocumentsKey()
    {
        return LuceneIndexService.DOC_INDEX_KEY;
    }

    void releaseAndRemoveWriter( String key, IndexWriter writer )
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
        finally
        {
            releaseWriteLock( key );
        }
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

    protected void fillDocument( Document document, long nodeId, Object value )
    {
        document.add( new Field( LuceneIndexService.DOC_ID_KEY,
            String.valueOf( nodeId ), Field.Store.YES,
            Field.Index.NOT_ANALYZED ) );
        document.add( new Field( LuceneIndexService.DOC_INDEX_KEY,
            value.toString(), Field.Store.NO, getIndexStrategy() ) );
    }

    protected Index getIndexStrategy()
    {
        return Field.Index.NOT_ANALYZED;
    }

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

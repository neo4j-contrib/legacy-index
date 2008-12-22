package org.neo4j.util.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.xaframework.XaCommand;
import org.neo4j.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaContainer;
import org.neo4j.impl.transaction.xaframework.XaDataSource;
import org.neo4j.impl.transaction.xaframework.XaTransaction;
import org.neo4j.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.impl.util.ArrayMap;

public class LuceneDataSource extends XaDataSource
{
    private final ArrayMap<String,IndexSearcher> indexSearchers = 
        new ArrayMap<String,IndexSearcher>( 6, true, true );

    private final XaContainer xaContainer;
    private final String storeDir;
    private final LockManager lockManager;
    private final LuceneIndexService service;

    private Map<String,LruCache<String,Iterable<Long>>> caching = 
        Collections.synchronizedMap( 
            new HashMap<String,LruCache<String,Iterable<Long>>>() );

    private byte[] branchId = null;

    public LuceneDataSource( Map<?,?> params ) throws InstantiationException
    {
        super( params );
        this.lockManager = (LockManager) params.get( LockManager.class );
        this.storeDir = (String) params.get( "dir" );
        this.service = ( LuceneIndexService )
            params.get( LuceneIndexService.class );
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
        XaCommandFactory cf = new LuceneCommandFactory();
        XaTransactionFactory tf = new LuceneTransactionFactory();
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
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new LuceneXaConnection( storeDir, xaContainer
            .getResourceManager(), branchId );
    }
    
    LuceneIndexService getIndexService()
    {
        return this.service;
    }
    
    private class LuceneCommandFactory extends XaCommandFactory
    {
        LuceneCommandFactory()
        {
            super();
        }

        @Override
        public XaCommand readCommand( FileChannel fileChannel, 
            ByteBuffer buffer ) throws IOException
        {
            return LuceneCommand.readCommand( fileChannel, buffer );
        }
    }
    private class LuceneTransactionFactory extends XaTransactionFactory
    {
        @Override
        public XaTransaction create( int identifier )
        {
            return service.createTransaction( identifier, this.getLogicalLog(),
                LuceneDataSource.this );
        }

        @Override
        public void lazyDoneWrite( List<Integer> identifiers )
        {
            // TODO Auto-generated method stub
        }
    }

    IndexSearcher acquireIndexSearcher( String key )
    {
        lockManager.getReadLock( key );
        IndexSearcher searcher = indexSearchers.get( key );
        if ( searcher == null )
        {
            try
            {
                Directory dir = FSDirectory.getDirectory( 
                    storeDir + "/" + key );
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

    void releaseIndexSearcher( String key, IndexSearcher searcher )
    {
        lockManager.releaseReadLock( key );
    }

    void removeIndexSearcher( String key )
    {
        lockManager.getWriteLock( key );
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
            lockManager.releaseWriteLock( key );
        }
    }

    synchronized IndexWriter getIndexWriter( String key )
    {
        try
        {
            lockManager.getWriteLock( key );
            Directory dir = FSDirectory.getDirectory( storeDir + "/" + key );
            return new IndexWriter( dir, this.service.getAnalyzer(),
                MaxFieldLength.UNLIMITED );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    void deleteDocumentUsingReader( IndexSearcher searcher, long nodeId,
        Object value )
    {
        if ( searcher == null )
        {
            return;
        }
        this.service.deleteDocuments( searcher, nodeId, value );
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
            lockManager.releaseWriteLock( key );
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

    @Override
    public byte[] getBranchId()
    {
        return branchId;
    }

    @Override
    public void setBranchId( byte[] branchId )
    {
        this.branchId = branchId;
    }
    
    void fillDocument( Document document, long nodeId, Object value )
    {
        this.service.fillDocument( document, nodeId, value );
    }
}
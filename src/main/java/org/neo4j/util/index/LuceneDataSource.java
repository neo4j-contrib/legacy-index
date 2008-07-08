package org.neo4j.util.index;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
    final ArrayMap<String,IndexSearcher> indexSearchers = 
        new ArrayMap<String,IndexSearcher>( 6, true, true );
    
    private final XaContainer xaContainer;
    private final String storeDir;

    private final LockManager lockManager;
    
    private Map<String, LruCache<Object, Iterable<Long>>> caching =
        Collections.synchronizedMap(
            new HashMap<String, LruCache<Object, Iterable<Long>>>() );
    
    private byte[] branchId = null;
    
    public LuceneDataSource( Map<?,?> params ) 
        throws InstantiationException
    {
        super( params );
        this.lockManager = (LockManager) params.get( LockManager.class );
        storeDir = (String) params.get( "dir" );
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
                throw new RuntimeException( "Unable to create directory " + 
                    dir, e );
            }
        }
        
        XaCommandFactory cf = new LuceneCommandFactory();
        XaTransactionFactory tf = new LuceneTransactionFactory( this );
        
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
                throw new IOException( "Unable to create directory path[" + 
                    dirs + "] for Neo store." );
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
        return new LuceneXaConnection( storeDir, 
            xaContainer.getResourceManager(), branchId );
    }
    
    private static class LuceneCommandFactory extends XaCommandFactory
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
    
    private static class LuceneTransactionFactory extends XaTransactionFactory
    {
        private final LuceneDataSource luceneDs;
        
        LuceneTransactionFactory( LuceneDataSource luceneDs )
        {
            this.luceneDs = luceneDs;
        }
        
        @Override
        public XaTransaction create( int identifier )
        {
            return new LuceneTransaction( identifier, getLogicalLog(), 
                luceneDs );
        }

        @Override
        public void lazyDoneWrite( List<Integer> identifiers )
        {
            // TODO Auto-generated method stub
        }
    }

    private static final Analyzer DEFAULT_ANALYZER = new DefaultAnalyzer();
    
    private static class DefaultAnalyzer extends Analyzer
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
        }
    }
    
    IndexSearcher getIndexSearcher( String key )
    {
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
    
    IndexSearcher removeIndexSearcher( String key )
    {
        return indexSearchers.remove( key );
    }
    
    synchronized IndexWriter getIndexWriter( String key )
    {
        try
        {
            lockManager.getWriteLock( key );
            Directory dir = FSDirectory.getDirectory( 
                storeDir + "/" + key );
            return new IndexWriter( dir, false, DEFAULT_ANALYZER );
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
        Query query = new TermQuery( new Term( "index", value.toString() ) );
        try
        {
            Hits hits = searcher.search( query );
            for ( int i = 0; i < hits.length(); i++ )
            {
                Document document = hits.doc( 0 );
                int foundId = Integer.parseInt(
                    document.getField( "id" ).stringValue() );
                if ( nodeId == foundId )
                {
                    int docNum = hits.id( i );
                    searcher.getIndexReader().deleteDocument( docNum );
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to delete for " + nodeId +"," + 
                "," + value + " using" + searcher, e );
        }
    }

    void releaseAndRemoveWriter( String key, IndexWriter writer )
    {
        try
        {
            writer.close();
        }
        catch ( CorruptIndexException e )
        {
            throw new RuntimeException( "Unable to close lucene writer " + 
                writer, e );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to close lucene writer " + 
                writer, e );
        }
        finally
        {
            lockManager.releaseWriteLock( key );
        }
    }

    public LruCache<Object,Iterable<Long>> getFromCache( String key )
    {
        return caching.get( key );
    }

    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        this.caching.put( key, new LruCache<Object, Iterable<Long>>(
            key, maxNumberOfCachedEntries, null ) );
    }

    void invalidateCache( String key, Object value )
    {
        LruCache<Object, Iterable<Long>> cache = caching.get( key );
        if ( cache != null )
        {
            cache.remove( value );
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
}
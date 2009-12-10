package org.neo4j.util.index;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.api.core.Node;
import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.impl.batchinsert.BatchInserter;
import org.neo4j.impl.util.ArrayMap;
import org.neo4j.impl.util.FileUtils;

/**
 * A default implementation of {@link LuceneIndexBatchInserter}.
 */
public class LuceneIndexBatchInserterImpl implements LuceneIndexBatchInserter
{
    private final String storeDir;
    private final BatchInserter neo;

    private final ArrayMap<String,IndexWriter> indexWriters = 
        new ArrayMap<String,IndexWriter>( 6, false, false );
//    private final ArrayMap<String, LruCache<String, Collection<Long>>> cache =
//        new ArrayMap<String, LruCache<String, Collection<Long>>>();

    private final Analyzer fieldAnalyzer = new Analyzer()
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
        }
    };
    
    private IndexService asIndexService;
    
    /**
     * @param neo the {@link BatchInserter} to use.
     */
    public LuceneIndexBatchInserterImpl( BatchInserter neo )
    {
        this.neo = neo;
        this.storeDir = fixPath( neo.getStore() + "/" + getDirName() );
        this.asIndexService = new AsIndexService();
    }
    
    protected String getDirName()
    {
        return LuceneIndexService.DIR_NAME;
    }
    
    private String fixPath( String dir )
    {
        String store = FileUtils.fixSeparatorsInPath( dir );
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
    
    private Directory instantiateDirectory( String key ) throws IOException
    {
        return FSDirectory.open( new File( storeDir + "/" + key ) );
    }
    
    private IndexWriter getWriter( String key, boolean allowCreate )
    {
        IndexWriter writer = indexWriters.get( key );
        if ( writer == null && allowCreate )
        {
            try
            {
                Directory dir = instantiateDirectory( key );
                writer = new IndexWriter( dir, fieldAnalyzer,
                    MaxFieldLength.UNLIMITED );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            indexWriters.put( key, writer );
        }
        return writer;
    }
    
    public void index( long node, String key, Object value )
    {
        IndexWriter writer = getWriter( key, true );
        Document document = new Document();
        fillDocument( document, node, key, value );
        try
        {
            writer.addDocument( document );
//            addToCache( node, key, value );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
//    private void addToCache( long node, String key, Object value )
//    {
//        if ( !useCache() )
//        {
//            return;
//        }
//        
//        LruCache<String, Collection<Long>> keyCache = this.cache.get( key );
//        if ( keyCache == null )
//        {
//            keyCache = new LruCache<String, Collection<Long>>(
//                key, getMaxCacheSizePerKey(), null );
//            cache.put( key, keyCache );
//        }
//        Collection<Long> ids = keyCache.get( value.toString() );
//        if ( ids == null )
//        {
//            ids = new ArrayList<Long>();
//            keyCache.put( value.toString(), ids );
//        }
//        ids.add( node );
//    }

    protected void fillDocument( Document document, long nodeId, String key,
        Object value )
    {
        document.add( new Field( LuceneIndexService.DOC_ID_KEY,
            String.valueOf( nodeId ), Field.Store.YES,
            Field.Index.NOT_ANALYZED ) );
        document.add( new Field( LuceneIndexService.DOC_INDEX_KEY,
            value.toString(), Field.Store.NO, getIndexStrategy() ) );
    }
    
    protected Field.Index getIndexStrategy()
    {
        return Field.Index.NOT_ANALYZED;
    }
    
    public void shutdown()
    {
        for ( IndexWriter writer : indexWriters.values() )
        {
            try
            {
                writer.close();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
    
//    /**
//     * @return {@code true} if this index service should use caching,
//     * otherwise {@code false}. Override this in your instance to change its
//     * behaviour.
//     */
//    public boolean useCache()
//    {
//        return true;
//    }
    
//    /**
//     * The cache used in this index service is a LRU cache, which mean that
//     * only the N most recent entries are kept in it.
//     * 
//     * @return the size of the LRU cache per key. Override this in your
//     * instance to change its behaviour.
//     */
//    public int getMaxCacheSizePerKey()
//    {
//        return 20000;
//    }

    public IndexHits<Long> getNodes( String key, Object value )
    {
//        if ( useCache() )
//        {
//            LruCache<String, Collection<Long>> keyCache = cache.get( key );
//            if ( keyCache != null )
//            {
//                Collection<Long> ids = keyCache.get( value.toString() );
//                if ( ids != null )
//                {
//                    return new SimpleIndexHits<Long>( ids, ids.size() );
//                }
//            }
//        }
        
        Set<Long> nodeSet = new HashSet<Long>();
        IndexWriter writer = getWriter( key, false );
        if ( writer == null )
        {
            return new SimpleIndexHits<Long>(
                Collections.<Long>emptyList(), 0 );
        }
        
        try
        {
            Query query = formQuery( key, value );
            IndexReader indexReader = writer.getReader();
            IndexSearcher indexSearcher = new IndexSearcher( indexReader );
            Hits hits = indexSearcher.search( query );
            for ( int i = 0; i < hits.length(); i++ )
            {
                Document document = hits.doc( i );
                long id = Long.parseLong( document.getField(
                    LuceneIndexService.DOC_ID_KEY ).stringValue() );
//                addToCache( id, key, value );
                nodeSet.add( id );
            }
            indexSearcher.close();
            indexReader.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return new SimpleIndexHits<Long>( nodeSet, nodeSet.size() );
    }
    
    protected Query formQuery( String key, Object value )
    {
        return new TermQuery( new Term( LuceneIndexService.DOC_INDEX_KEY, 
            value.toString() ) );
    }
    
    public void optimize()
    {
        try
        {
//            List<IndexWriter> writers = new ArrayList<IndexWriter>();
            for ( IndexWriter writer : indexWriters.values() )
            {
//                closeReader( writer );
                writer.optimize( true );
//                writers.add( writer );
            }
//            indexWriters.clear();
//            for ( IndexWriter writer : writers )
//            {
//                writer.close();
//            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public long getSingleNode( String key, Object value )
    {
        Iterator<Long> nodes = getNodes( key, value ).iterator();
        long node = nodes.hasNext() ? nodes.next() : -1;
        while ( nodes.hasNext() )
        {
            if ( !nodes.next().equals( node ) )
            {
                throw new RuntimeException( "More than one node for " + key + "="
                    + value );
            }
        }
        return node;
    }

    public IndexService getIndexService()
    {
        return asIndexService;
    }
    
    private class AsIndexService implements IndexService
    {
        public IndexHits<Node> getNodes( String key, Object value )
        {
            IndexHits<Long> ids = LuceneIndexBatchInserterImpl.this.getNodes(
                key, value );
            Iterable<Node> nodes = new IterableWrapper<Node, Long>( ids )
            {
                @Override
                protected Node underlyingObjectToObject( Long id )
                {
                    return neo.getNeoService().getNodeById( id );
                }
            };
            return new SimpleIndexHits<Node>( nodes, ids.size() );
        }

        public Node getSingleNode( String key, Object value )
        {
            long id =
                LuceneIndexBatchInserterImpl.this.getSingleNode( key, value );
            return id == -1 ? null : neo.getNeoService().getNodeById( id );
        }

        public void index( Node node, String key, Object value )
        {
            LuceneIndexBatchInserterImpl.this.index( node.getId(), key, value );
        }

        public void removeIndex( Node node, String key, Object value )
        {
            throw new UnsupportedOperationException();
        }

        public void setIsolation( Isolation level )
        {
            throw new UnsupportedOperationException();
        }

        public void shutdown()
        {
            LuceneIndexBatchInserterImpl.this.shutdown();
        }
    }
}

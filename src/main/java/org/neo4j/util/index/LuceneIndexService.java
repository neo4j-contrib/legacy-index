package org.neo4j.util.index;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.util.ArrayMap;

// TODO: 
// o Move LuceneTransaction to its own file
// o Fix remove index bug
// o Run optimize when starting up
public class LuceneIndexService extends GenericIndexService
{
    private final ArrayMap<String,IndexSearcher> indexSearchers = 
        new ArrayMap<String,IndexSearcher>( 6, true, true );
    
    private final ThreadLocal<LuceneTransaction> luceneTransactions
        = new ThreadLocal<LuceneTransaction>();
    private final LockManager lockManager;
    private final TransactionManager txManager;

    
    private static class WriterLock
    {
        private final String key;
        
        WriterLock( String key )
        {
            this.key = key;
        }
        
        private String getKey()
        {
            return key;
        }
        
        @Override
        public int hashCode()
        {
            return key.hashCode();
        }
        
        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof WriterLock) )
            {
                return false;
            }
            return this.key.equals( ((WriterLock) o).getKey() );
        }
    }
    
    private class LuceneTransaction implements Synchronization
    {
        private final Map<WriterLock,IndexWriter> writers = 
            new HashMap<WriterLock,IndexWriter>();
        
        private final Map<String,Map<Object,Long[]>> txIndexed = 
            new HashMap<String,Map<Object,Long[]>>();

        private final Map<String,Map<Object,Long[]>> txRemoved = 
            new HashMap<String,Map<Object,Long[]>>();
        
        IndexWriter getWriterForLock( WriterLock lock )
        {
            return writers.get( lock );
        }

        void addWriter( WriterLock lock, IndexWriter writer )
        {
            writers.put( lock, writer );
        }

        void index( Node node, String key, Object value )
        {
            delRemovedIndex( node, key, value );
            Map<Object,Long[]> keyIndex = txIndexed.get( key );
            if ( keyIndex == null )
            {
                keyIndex = new HashMap<Object,Long[]>();
                txIndexed.put( key, keyIndex );
            }
            Long nodeIds[] = keyIndex.get( value );
            if ( nodeIds == null )
            {
                nodeIds = new Long[1];
                nodeIds[0] = node.getId();
            }
            else
            {
                Long newIds[] = new Long[nodeIds.length + 1 ];
                boolean dupe = false;
                long nodeId = node.getId();
                for ( int i = 0; i < nodeIds.length; i++ )
                {
                    if ( nodeIds[i] == nodeId )
                    {
                        dupe = true;
                        break;
                    }
                    newIds[i] = nodeIds[i];
                }
                if ( !dupe )
                {
                    newIds[nodeIds.length] = node.getId();
                    nodeIds = newIds;
                }
            }
            keyIndex.put( value, nodeIds );
        }
        
        void removeIndex( Node node, String key, Object value )
        {
            delAddedIndex( node, key, value );
            Map<Object,Long[]> keyIndex = txRemoved.get( key );
            if ( keyIndex == null )
            {
                keyIndex = new HashMap<Object,Long[]>();
                txRemoved.put( key, keyIndex );
            }
            Long nodeIds[] = keyIndex.get( value );
            if ( nodeIds == null )
            {
                nodeIds = new Long[1];
                nodeIds[0] = node.getId();
            }
            else
            {
                Long newIds[] = new Long[nodeIds.length + 1 ];
                boolean dupe = false;
                long nodeId = node.getId();
                for ( int i = 0; i < nodeIds.length; i++ )
                {
                    if ( nodeIds[i] == nodeId )
                    {
                        dupe = true;
                        break;
                    }
                    newIds[i] = nodeIds[i];
                }
                if ( !dupe )
                {
                    newIds[nodeIds.length] = node.getId();
                    nodeIds = newIds;
                }
            }
            keyIndex.put( value, nodeIds );
        }
        
        void delRemovedIndex( Node node, String key, Object value )
        {
            Map<Object,Long[]> keyIndex = txRemoved.get( key );
            if ( keyIndex == null )
            {
                return;
            }
            Long nodeIds[] = keyIndex.get( value );
            if ( nodeIds == null )
            {
                return;
            }
            Long newIds[] = new Long[nodeIds.length - 1 ];
            long nodeId = node.getId();
            int index = 0;
            for ( int i = 0; i < nodeIds.length; i++ )
            {
                if ( nodeIds[i] != nodeId )
                {
                    newIds[index++] = nodeIds[i];
                }
            }
            if ( newIds.length == 0 )
            {
                keyIndex.remove( value );
            }
            else
            {
                keyIndex.put( value, newIds );
            }
        }
        
        void delAddedIndex( Node node, String key, Object value )
        {
            Map<Object,Long[]> keyIndex = txIndexed.get( key );
            if ( keyIndex == null )
            {
                return;
            }
            Long nodeIds[] = keyIndex.get( value );
            if ( nodeIds == null )
            {
                return;
            }
            Long newIds[] = new Long[nodeIds.length - 1 ];
            long nodeId = node.getId();
            int index = 0;
            for ( int i = 0; i < nodeIds.length; i++ )
            {
                if ( nodeIds[i] != nodeId )
                {
                    newIds[index++] = nodeIds[i];
                }
            }
            if ( newIds.length == 0 )
            {
                keyIndex.remove( value );
            }
            else
            {
                keyIndex.put( value, newIds );
            }
        }
        
        Set<Node> getDeletedNodesFor( String key, Object value )
        {
            Map<Object,Long[]> keyIndex = txRemoved.get( key );
            if ( keyIndex != null )
            {
                Long[] nodeIds = keyIndex.get( value );
                if ( nodeIds != null )
                {
                    Set<Node> nodes = new HashSet<Node>();
                    for ( long nodeId : nodeIds )
                    {
                        try
                        {
                            nodes.add( getNeo().getNodeById( nodeId ) );
                        }
                        catch ( NotFoundException e )
                        { // ok deleted in this tx
                        }
                    }
                    return nodes;
                }
            }
            return Collections.EMPTY_SET;
        }
        
        List<Node> getNodesFor( String key, Object value )
        {
            Map<Object,Long[]> keyIndex = txIndexed.get( key );
            if ( keyIndex != null )
            {
                Long[] nodeIds = keyIndex.get( value );
                if ( nodeIds != null )
                {
                    List<Node> nodes = new LinkedList<Node>();
                    for ( long nodeId : nodeIds )
                    {
                        try
                        {
                            nodes.add( getNeo().getNodeById( nodeId ) );
                        }
                        catch ( NotFoundException e )
                        { // ok deleted in this tx
                        }
                    }
                    return nodes;
                }
            }
            return Collections.EMPTY_LIST;
        }
        
        public void afterCompletion( int status )
        {
            luceneTransactions.set( null );
            for ( Entry<WriterLock,IndexWriter> entry : writers.entrySet() )
            {
                IndexWriter writer = entry.getValue();
                WriterLock lock = entry.getKey();
                if ( status == Status.STATUS_COMMITTED )
                {
                    try
                    {
                        writer.close();
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                    IndexSearcher searcher = indexSearchers.remove( 
                        lock.getKey() );
                    if ( searcher != null )
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
                }
                else
                {
                    try
                    {
                        writer.abort();
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                }
                lockManager.releaseWriteLock( lock );
            }
        }

        public void beforeCompletion()
        {
        }
    }
    
    private static class DefaultAnalyzer extends Analyzer
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
        }
    }
    
    private static final Analyzer DEFAULT_ANALYZER = new DefaultAnalyzer();
    
    public LuceneIndexService( NeoService neo )
    {
        super ( neo );
        EmbeddedNeo embeddedNeo = ((EmbeddedNeo) neo);
        lockManager = embeddedNeo.getConfig().getLockManager();
        txManager = embeddedNeo.getConfig().getTxModule().getTxManager();
    }
    
    private synchronized IndexWriter getIndexWriter( String key )
    {
        try
        {
            Directory dir = FSDirectory.getDirectory( 
                "var/search/" + key );
            return new IndexWriter( dir, false, DEFAULT_ANALYZER );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    protected void indexThisTx( Node node, String key, 
        Object value )
    {
        LuceneTransaction luceneTx = luceneTransactions.get();
        if ( luceneTx == null )
        {
            luceneTx = new LuceneTransaction();
            luceneTransactions.set( luceneTx );
            try
            {
                Transaction tx = txManager.getTransaction();
                tx.registerSynchronization( luceneTx );
            }
            catch ( SystemException e )
            {
                throw new IllegalStateException( "No transaciton running?", e );
            }
            catch ( RollbackException e )
            {
                throw new IllegalStateException( 
                    "Unable to register syncrhonization hook", e );
            }
        }
        WriterLock lock = new WriterLock( key );
        IndexWriter writer = luceneTx.getWriterForLock( lock );
        if ( writer == null )
        {
            lockManager.getWriteLock( lock );
            writer = getIndexWriter( key );
            luceneTx.addWriter( lock, writer );
        }
        Document document = new Document();
        document.add( new Field( "id", 
            String.valueOf( node.getId() ),
            Field.Store.YES, Field.Index.UN_TOKENIZED ) );
        document.add( new Field( "index", value.toString(),
            Field.Store.NO, Field.Index.UN_TOKENIZED ) );
        try
        {
            writer.addDocument( document );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        luceneTx.index( node, key, value );
    }
    
    private IndexSearcher getIndexSearcher( String key )
    {
        IndexSearcher searcher = indexSearchers.get( key );
        if ( searcher == null )
        {
            try
            {
                Directory dir = FSDirectory.getDirectory( 
                    "var/search/" + key );
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
    
    public Iterable<Node> getNodes( String key, Object value )
    {
        IndexSearcher searcher = getIndexSearcher( key );
        if ( searcher == null )
        {
            return Arrays.asList( new Node[0] );
        }
        Query query = new TermQuery( new Term( "index", value.toString() ) );
        try
        {
            Hits hits = searcher.search( query );
            List<Node> nodes = new LinkedList<Node>();
            for ( int i = 0; i < hits.length(); i++ )
            {
                Document document = hits.doc( i );
                try
                {
                    nodes.add( getNeo().getNodeById( Integer.parseInt(
                    document.getField( "id" ).stringValue() ) ) );
                }
                catch ( NotFoundException e )
                {
                    // deleted in this tx
                }
            }
            LuceneTransaction luceneTx = luceneTransactions.get();
            Set<Node> deletedNodes = Collections.EMPTY_SET;
            if ( luceneTx != null )
            {
                // add nodes that has been indexed in this tx
                List<Node> txNodes = luceneTx.getNodesFor( key, value );
                nodes.addAll( txNodes );
                deletedNodes = luceneTx.getDeletedNodesFor( key, value );
            }
            // remove dupes and deleted in same tx
            Set<Node> addedNodes = new HashSet<Node>();
            Iterator<Node> nodeItr = nodes.iterator();
            while ( nodeItr.hasNext() )
            {
                Node node = nodeItr.next();
                if ( !addedNodes.add( node ) || deletedNodes.contains( node ) )
                {
                    nodeItr.remove();
                }
            }
            return nodes;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to search for " + key + "," + 
                value, e );
        }
    }
    
    public Node getSingleNode( String key, Object value )
    {
        IndexSearcher searcher = getIndexSearcher( key );
        if ( searcher == null )
        {
            return null;
        }
        LuceneTransaction luceneTx = luceneTransactions.get();
        Set<Node> deletedNodes = Collections.EMPTY_SET;
        List<Node> addedNodes = Collections.EMPTY_LIST;
        if ( luceneTx != null )
        {
            addedNodes = luceneTx.getNodesFor( key, value );
            deletedNodes = luceneTx.getDeletedNodesFor( key, value );
            Iterator<Node> nodeItr = addedNodes.iterator();
            while ( nodeItr.hasNext() )
            {
                Node addedNode = nodeItr.next();
                if ( deletedNodes.contains( addedNode ) )
                {
                    nodeItr.remove();
                }
            }
        }
        Query query = new TermQuery( new Term( "index", value.toString() ) );
        try
        {
            Hits hits = searcher.search( query );
            if ( hits.length() == 1 )
            {
                Document document = hits.doc( 0 );
                Node node = getNeo().getNodeById( Integer.parseInt(
                    document.getField( "id" ).stringValue() ) );
                if ( deletedNodes.contains( node ) )
                {
                    node = null;
                }
                if ( addedNodes.size() == 0 )
                {
                    return node;
                }
                if ( addedNodes.size() == 1 )
                {
                    if ( node == null )
                    {
                        return addedNodes.get( 0 );
                    }
                    if ( node.equals( addedNodes.get( 0 ) ) )
                    {
                        return node;
                    }
                }
            }
            else if ( hits.length() == 0 )
            {
                if ( addedNodes.size() == 0 )
                {
                    return null;
                }
                if ( addedNodes.size() == 1 )
                {
                    return addedNodes.get( 0 );
                }
            }
            throw new RuntimeException( "More then one node found for: " +
                key + "," + value );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to search for " + key + "," + 
                value, e );
        }
    }

    protected void removeIndexThisTx( Node node, String key, Object value )
    {
        LuceneTransaction luceneTx = luceneTransactions.get();
        if ( luceneTx == null )
        {
            luceneTx = new LuceneTransaction();
            luceneTransactions.set( luceneTx );
            try
            {
                Transaction tx = txManager.getTransaction();
                tx.registerSynchronization( luceneTx );
            }
            catch ( SystemException e )
            {
                throw new IllegalStateException( "No transaciton running?", e );
            }
            catch ( RollbackException e )
            {
                throw new IllegalStateException( 
                    "Unable to register syncrhonization hook", e );
            }
        }
        WriterLock lock = new WriterLock( key );
        IndexWriter writer = luceneTx.getWriterForLock( lock );
        if ( writer == null )
        {
            lockManager.getWriteLock( lock );
            writer = getIndexWriter( key );
            luceneTx.addWriter( lock, writer );
        }
        Term term = new Term( new Long( node.getId() ).toString(), 
            value.toString().toLowerCase() );
        try
        {
            writer.deleteDocuments( term );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        luceneTx.removeIndex( node, key, value );
    }
    
    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
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
    }
}
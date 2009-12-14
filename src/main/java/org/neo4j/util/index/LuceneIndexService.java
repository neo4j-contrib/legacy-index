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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.commons.iterator.FilteringIterable;
import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.TxModule;
import org.neo4j.impl.util.ArrayMap;

/**
 * An implementation of {@link IndexService} which uses Lucene as backend.
 * Additional features to {@link IndexService} is:
 * <ul>
 *   <li>{@link #enableCache(String, int)} will enable a LRU cache for the
 *   specific key and will boost performance in performance-critical areas.</li>
 *   <li>{@link #getNodes(String, Object, Sort)} where you can pass in a
 *   {@link Sort} option to control in which order lucene returns the
 *   results</li>
 * </ul>
 * 
 * See more information at http://wiki.neo4j.org/content/Indexing_with_IndexService
 */
public class LuceneIndexService extends GenericIndexService
{
    protected static final String DOC_ID_KEY = "id";
    protected static final String DOC_INDEX_KEY = "index";
    protected static final String DIR_NAME = "lucene";
    
    private final TransactionManager txManager;
    private final ConnectionBroker broker;
    private final LuceneDataSource xaDs;

    /**
     * @param neo the {@link NeoService} to use.
     */
    public LuceneIndexService( NeoService neo )
    {
        super( neo );
        EmbeddedNeo embeddedNeo = ((EmbeddedNeo) neo);
        String luceneDirectory = 
            embeddedNeo.getConfig().getTxModule().getTxLogDirectory() +
                "/" + getDirName();
        TxModule txModule = embeddedNeo.getConfig().getTxModule();
        txManager = txModule.getTxManager();
        byte resourceId[] = getXaResourceId();
        Map<Object,Object> params = getDefaultParams();
        params.put( "dir", luceneDirectory );
        params.put( LockManager.class, 
            embeddedNeo.getConfig().getLockManager() );
        xaDs = ( LuceneDataSource ) txModule.registerDataSource( getDirName(),
            getDataSourceClass().getName(), resourceId, params, true );
        broker = new ConnectionBroker( txManager, xaDs );
        xaDs.setIndexService( this );
    }
    
    protected Class<? extends LuceneDataSource> getDataSourceClass()
    {
        return LuceneDataSource.class;
    }
    
    protected String getDirName()
    {
        return DIR_NAME;
    }
    
    protected byte[] getXaResourceId()
    {
        return "162373".getBytes();
    }
    
    private Map<Object,Object> getDefaultParams()
    {
        Map<Object,Object> params = new HashMap<Object,Object>();
        params.put( LuceneIndexService.class, this );
        return params;
    }

    /**
     * Enables an LRU cache for a specific index (specified by {@code key})
     * so that the {@code maxNumberOfCachedEntries} number of results found with
     * {@link #getNodes(String, Object)} are cached for faster consecutive
     * lookups. It's prefered to enable cache at construction time.
     * 
     * @param key the index to enable cache for.
     * @param maxNumberOfCachedEntries the max size of the cache before old
     * ones are flushed from the cache.
     */
    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        xaDs.enableCache( key, maxNumberOfCachedEntries );
    }

    @Override
    protected void indexThisTx( Node node, String key, Object value )
    {
        getConnection().index( node, key, value );
    }
    
    public IndexHits<Node> getNodes( String key, Object value )
    {
        return getNodes( key, value, null );
    }
    
    /**
     * Just like {@link #getNodes(String, Object)}, but with sorted result.
     * 
     * @param key the index to query.
     * @param value the value to query for.
     * @param sortingOrNull lucene sorting behaviour for the result. Ignored
     * if {@code null}.
     * @return nodes that has been indexed with key and value, optionally
     * sorted with {@code sortingOrNull}.
     */
    public IndexHits<Node> getNodes( String key, Object value,
        Sort sortingOrNull )
    {
        List<Long> nodeIds = new ArrayList<Long>();
        LuceneTransaction luceneTx = getConnection().getLuceneTx();
        Set<Long> addedNodes = Collections.emptySet();
        Set<Long> deletedNodes = Collections.emptySet();
        if ( luceneTx != null )
        {
            addedNodes = luceneTx.getNodesFor( key, value );
            nodeIds.addAll( addedNodes );
            deletedNodes = luceneTx.getDeletedNodesFor( key, value );
        }
        xaDs.getReadLock();
        try
        {
            IndexSearcher searcher = xaDs.getIndexSearcher( key );
            if ( searcher != null )
            {
                LruCache<String,Collection<Long>> cachedNodesMap = 
                    xaDs.getFromCache( key );
                boolean foundInCache = false;
                String valueAsString = value.toString();
                if ( cachedNodesMap != null )
                {
                    Collection<Long> cachedNodes =
                        cachedNodesMap.get( valueAsString );
                    if ( cachedNodes != null )
                    {
                        foundInCache = true;
                        nodeIds.addAll( cachedNodes );
                    }
                }
                if ( !foundInCache )
                {
                    Iterable<Long> searchedNodeIds = searchForNodes( key, value,
                        sortingOrNull, deletedNodes );
                    ArrayList<Long> readNodeIds = new ArrayList<Long>();
                    for ( Long readNodeId : searchedNodeIds )
                    {
                        nodeIds.add( readNodeId );
                        readNodeIds.add( readNodeId );
                    }
                    if ( cachedNodesMap != null )
                    {
                        cachedNodesMap.put( valueAsString, readNodeIds );
                    }
                }
            }
        }
        finally
        {
            xaDs.releaseReadLock();
        }
        
        return new SimpleIndexHits<Node>(
            instantiateIdToNodeIterable( nodeIds ), nodeIds.size() );
    }
    
    protected Iterable<Node> instantiateIdToNodeIterable( Iterable<Long> ids )
    {
        ids = new FilteringIterable<Long>( ids )
        {
            private final Set<Long> passedIds = new HashSet<Long>();
            
            @Override
            protected boolean passes( Long id )
            {
                return passedIds.add( id );
            }
        };
        
        return new IterableWrapper<Node, Long>( ids )
        {
            @Override
            protected Node underlyingObjectToObject( Long id )
            {
                return getNeo().getNodeById( id );
            }
        };
    }
    
    protected Query formQuery( String key, Object value )
    {
        return new TermQuery( new Term( DOC_INDEX_KEY, value.toString() ) );
    }

    private Iterable<Long> searchForNodes( String key, Object value,
        Sort sortingOrNull, Set<Long> deletedNodes )
    {
        Query query = formQuery( key, value );
        xaDs.getReadLock();
        try
        {
            IndexSearcher searcher = xaDs.getIndexSearcher( key );
            ArrayList<Long> nodes = new ArrayList<Long>();
            Hits hits = sortingOrNull != null ?
                searcher.search( query, sortingOrNull ) :
                searcher.search( query );
            for ( int i = 0; i < hits.length(); i++ )
            {
                Document document = hits.doc( i );
                try
                {
                    long id = Long.parseLong( document.getField( DOC_ID_KEY )
                        .stringValue() );
                    if ( !deletedNodes.contains( id ) )
                    {
                        nodes.add( id );
                    }
                }
                catch ( NotFoundException e )
                {
                    // deleted in this tx
                }
            }
            return nodes;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to search for " + key + ","
                + value, e );
        }
        finally
        {
            xaDs.releaseReadLock();
        }
    }

    public Node getSingleNode( String key, Object value )
    {
        Iterator<Node> nodes = getNodes( key, value ).iterator();
        Node node = nodes.hasNext() ? nodes.next() : null;
        if ( nodes.hasNext() )
        {
            throw new RuntimeException( "More than one node for " + key + "="
                + value );
        }
        return node;
    }

    @Override
    protected void removeIndexThisTx( Node node, String key, Object value )
    {
        getConnection().removeIndex( node, key, value );
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        EmbeddedNeo embeddedNeo = ( ( EmbeddedNeo ) getNeo() );
        TxModule txModule = embeddedNeo.getConfig().getTxModule();
        if ( txModule.getXaDataSourceManager().hasDataSource( getDirName() ) )
        {
            txModule.getXaDataSourceManager().unregisterDataSource(
                getDirName() );
        }
        xaDs.close();
    }

    LuceneXaConnection getConnection()
    {
        return broker.acquireResourceConnection();
    }

    private static class ConnectionBroker
    {
        private final ArrayMap<Transaction,LuceneXaConnection> txConnectionMap =
            new ArrayMap<Transaction,LuceneXaConnection>( 5, true, true );
        private final TransactionManager transactionManager;
        private final LuceneDataSource xaDs;

        ConnectionBroker( TransactionManager transactionManager,
            LuceneDataSource xaDs )
        {
            this.transactionManager = transactionManager;
            this.xaDs = xaDs;
        }

        LuceneXaConnection acquireResourceConnection()
        {
            LuceneXaConnection con = null;
            Transaction tx = this.getCurrentTransaction();
            con = txConnectionMap.get( tx );
            if ( con == null )
            {
                try
                {
                    con = (LuceneXaConnection) xaDs.getXaConnection();
                    if ( !tx.enlistResource( con.getXaResource() ) )
                    {
                        throw new RuntimeException( "Unable to enlist '"
                            + con.getXaResource() + "' in " + tx );
                    }
                    tx.registerSynchronization( new TxCommitHook( tx ) );
                    txConnectionMap.put( tx, con );
                }
                catch ( javax.transaction.RollbackException re )
                {
                    String msg = "The transaction is marked for rollback only.";
                    throw new RuntimeException( msg, re );
                }
                catch ( javax.transaction.SystemException se )
                {
                    String msg = 
                        "TM encountered an unexpected error condition.";
                    throw new RuntimeException( msg, se );
                }
            }
            return con;
        }

        void releaseResourceConnectionsForTransaction( Transaction tx )
            throws NotInTransactionException
        {
            LuceneXaConnection con = txConnectionMap.remove( tx );
            if ( con != null )
            {
                con.destroy();
            }
        }

        void delistResourcesForTransaction() throws NotInTransactionException
        {
            Transaction tx = this.getCurrentTransaction();
            LuceneXaConnection con = txConnectionMap.get( tx );
            if ( con != null )
            {
                try
                {
                    tx.delistResource( con.getXaResource(),
                        XAResource.TMSUCCESS );
                }
                catch ( IllegalStateException e )
                {
                    throw new RuntimeException(
                        "Unable to delist lucene resource from tx", e );
                }
                catch ( SystemException e )
                {
                    throw new RuntimeException(
                        "Unable to delist lucene resource from tx", e );
                }
            }
        }

        private Transaction getCurrentTransaction()
            throws NotInTransactionException
        {
            try
            {
                Transaction tx = transactionManager.getTransaction();
                if ( tx == null )
                {
                    throw new NotInTransactionException(
                        "No transaction found for current thread" );
                }
                return tx;
            }
            catch ( SystemException se )
            {
                throw new NotInTransactionException(
                    "Error fetching transaction for current thread", se );
            }
        }

        private class TxCommitHook implements Synchronization
        {
            private final Transaction tx;

            TxCommitHook( Transaction tx )
            {
                this.tx = tx;
            }

            public void afterCompletion( int param )
            {
                releaseResourceConnectionsForTransaction( tx );
            }

            public void beforeCompletion()
            {
                delistResourcesForTransaction();
            }
        }
    }
}
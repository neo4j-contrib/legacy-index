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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.neo4j.commons.iterator.CombiningIterator;
import org.neo4j.commons.iterator.IteratorAsIterable;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.ReadOnlyIndexException;
import org.neo4j.index.impl.GenericIndexService;
import org.neo4j.index.impl.IdToNodeIterator;
import org.neo4j.index.impl.SimpleIndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.impl.cache.LruCache;

/**
 * A version of {@link LuceneIndexService} which is read-only and will throw
 * {@link ReadOnlyIndexException} in
 * {@link IndexService#index(Node, String, Object)} and
 * {@link IndexService#removeIndex(Node, String, Object)}. See
 * {@link EmbeddedReadOnlyGraphDatabase}.
 */
public class LuceneReadOnlyIndexService extends GenericIndexService
{
    protected static final String DOC_ID_KEY = "id";
    protected static final String DOC_INDEX_KEY = "index";

    private final LuceneReadOnlyDataSource xaDs;
    private int lazynessThreshold = 100;

    /**
     * @param graphDb the {@link GraphDatabaseService} to use.
     */
    public LuceneReadOnlyIndexService( GraphDatabaseService graphDb )
    {
        super( graphDb );
        String luceneDirectory;
        if ( graphDb instanceof EmbeddedReadOnlyGraphDatabase )
        {
            EmbeddedReadOnlyGraphDatabase embeddedGraphDb = ( (EmbeddedReadOnlyGraphDatabase) graphDb );
            luceneDirectory = embeddedGraphDb.getStoreDir() + "/"
                              + getDirName();
        }
        else
        {
            EmbeddedGraphDatabase embeddedGraphDb = ( (EmbeddedGraphDatabase) graphDb );
            luceneDirectory = embeddedGraphDb.getStoreDir() + "/"
                              + getDirName();
        }
        xaDs = new LuceneReadOnlyDataSource( luceneDirectory );
    }

    protected String getDirName()
    {
        return "lucene";
    }

    protected Field.Index getIndexStrategy()
    {
        return Field.Index.NOT_ANALYZED;
    }

    /**
     * Enables an LRU cache for a specific index (specified by {@code key}) so
     * that the {@code maxNumberOfCachedEntries} number of results found with
     * {@link #getNodes(String, Object)} are cached for faster consecutive
     * lookups. It's preferred to enable cache at construction time.
     * 
     * @param key the index to enable cache for.
     * @param maxNumberOfCachedEntries the max size of the cache before old ones
     *            are flushed from the cache.
     * @see LuceneIndexService#enableCache(String, int)
     */
    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        xaDs.enableCache( key, maxNumberOfCachedEntries );
    }

    @Override
    protected void indexThisTx( Node node, String key, Object value )
    {
        throw new ReadOnlyIndexException();
    }

    /**
     * (Copied from {@link LuceneIndexService#setLazySearchResultThreshold(int)}
     * )
     * 
     * Sets the threshold for when a result is considered big enough to skip
     * cache and be returned as a fully lazy iterator so that
     * {@link #getNodes(String, Object)} will return very fast and all the
     * reading and fetching of nodes is done lazily before each step in the
     * iteration of the returned result. The default value is
     * {@link LuceneIndexService#DEFAULT_LAZY_SEARCH_RESULT_THRESHOLD}.
     * 
     * @param numberOfHitsBeforeLazyLoading the threshold where results which
     *            are bigger than that threshold becomes lazy.
     */
    public void setLazySearchResultThreshold( int numberOfHitsBeforeLazyLoading )
    {
        this.lazynessThreshold = numberOfHitsBeforeLazyLoading;
        xaDs.invalidateCache();
    }

    /**
     * (Copied from {@link LuceneIndexService#getLazySearchResultThreshold()}
     * 
     * Returns the threshold for when a result is considered big enough to skip
     * cache and be returned as a fully lazy iterator so that
     * {@link #getNodes(String, Object)} will return very fast and all the
     * reading and fetching of nodes is done lazily before each step in the
     * iteration of the returned result. The default value is
     * {@link LuceneIndexService#DEFAULT_LAZY_SEARCH_RESULT_THRESHOLD}.
     * 
     * @return the threshold for when a result is considered big enough to be
     *         returned as a lazy iteration.
     */
    public int getLazySearchResultThreshold()
    {
        return this.lazynessThreshold;
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
     * @param sortingOrNull lucene sorting behaviour for the result. Ignored if
     *            {@code null}.
     * @return nodes that has been indexed with key and value, optionally sorted
     *         with {@code sortingOrNull}.
     */
    public IndexHits<Node> getNodes( String key, Object value,
            Sort sortingOrNull )
    {
        List<Long> nodeIds = new ArrayList<Long>();
        IndexSearcher searcher = xaDs.getIndexSearcher( key );
        Iterator<Long> nodeIdIterator = null;
        Integer nodeIdIteratorSize = null;
        if ( searcher != null )
        {
            LruCache<String, Collection<Long>> cachedNodesMap = xaDs.getFromCache( key );
            boolean foundInCache = false;
            String valueAsString = value.toString();
            if ( cachedNodesMap != null )
            {
                Collection<Long> cachedNodes = cachedNodesMap.get( valueAsString );
                if ( cachedNodes != null )
                {
                    foundInCache = true;
                    nodeIds.addAll( cachedNodes );
                }
            }
            if ( !foundInCache )
            {
                DocToIdIterator searchedNodeIds = searchForNodes( key, value,
                        sortingOrNull );
                if ( searchedNodeIds.size() >= this.lazynessThreshold )
                {
                    if ( cachedNodesMap != null )
                    {
                        cachedNodesMap.remove( valueAsString );
                    }

                    Collection<Iterator<Long>> iterators = new ArrayList<Iterator<Long>>();
                    iterators.add( nodeIds.iterator() );
                    iterators.add( searchedNodeIds );
                    nodeIdIterator = new CombiningIterator<Long>( iterators );
                    nodeIdIteratorSize = nodeIds.size()
                                         + searchedNodeIds.size();
                }
                else
                {
                    ArrayList<Long> readNodeIds = new ArrayList<Long>();
                    while ( searchedNodeIds.hasNext() )
                    {
                        Long readNodeId = searchedNodeIds.next();
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

        if ( nodeIdIterator == null )
        {
            nodeIdIterator = nodeIds.iterator();
            nodeIdIteratorSize = nodeIds.size();
        }
        return new SimpleIndexHits<Node>( new IteratorAsIterable<Node>(
                instantiateIdToNodeIterator( nodeIdIterator ) ),
                nodeIdIteratorSize );
    }

    protected Iterator<Node> instantiateIdToNodeIterator(
            final Iterator<Long> ids )
    {
        return new IdToNodeIterator( ids, getGraphDb() );
    }

    protected Query formQuery( String key, Object value )
    {
        return new TermQuery( new Term( DOC_INDEX_KEY, value.toString() ) );
    }

    private DocToIdIterator searchForNodes( String key, Object value,
            Sort sortingOrNull )
    {
        Query query = formQuery( key, value );
        try
        {
            IndexSearcher searcher = xaDs.getIndexSearcher( key );
            Hits hits = sortingOrNull != null ? searcher.search( query,
                    sortingOrNull ) : searcher.search( query );
            return new DocToIdIterator( new HitsIterator( hits ),
                    Collections.<Long>emptyList(), null );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to search for " + key + ","
                                        + value, e );
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
        throw new ReadOnlyIndexException();
    }
    
    public void removeIndex( Node node, String key )
    {
        throw new ReadOnlyIndexException();
    }
    
    public void removeIndex( String key )
    {
        throw new ReadOnlyIndexException();
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        xaDs.close();
    }
}

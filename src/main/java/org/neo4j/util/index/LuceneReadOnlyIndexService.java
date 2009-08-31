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
package org.neo4j.util.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.EmbeddedReadOnlyNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.impl.cache.LruCache;

public class LuceneReadOnlyIndexService extends GenericIndexService
{
    protected static final String DOC_ID_KEY = "id";
    protected static final String DOC_INDEX_KEY = "index";

    private final LuceneReadOnlyDataSource xaDs;
    private Sort sorting;

    public LuceneReadOnlyIndexService( NeoService neo )
    {
        super( neo );
        String luceneDirectory;
        if ( neo instanceof EmbeddedReadOnlyNeo )
        {
            EmbeddedReadOnlyNeo embeddedNeo = ((EmbeddedReadOnlyNeo) neo);
            luceneDirectory = 
                embeddedNeo.getStoreDir() + "/" + getDirName();
        }
        else
        {
            EmbeddedNeo embeddedNeo = ((EmbeddedNeo) neo);
            luceneDirectory = 
                embeddedNeo.getStoreDir() + "/" + getDirName();
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
    
    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        xaDs.enableCache( key, maxNumberOfCachedEntries );
    }

    @Override
    protected void indexThisTx( Node node, String key, Object value )
    {
        throw new ReadOnlyIndexException();
    }
    
    public Iterable<Node> getNodes( String key, Object value )
    {
        List<Node> nodes = new ArrayList<Node>();
        IndexSearcher searcher = xaDs.getIndexSearcher( key );
        if ( searcher != null )
        {
            LruCache<String,Iterable<Long>> cachedNodesMap = 
                xaDs.getFromCache( key );
            boolean foundInCache = false;
            String valueAsString = value.toString();
            if ( cachedNodesMap != null )
            {
                Iterable<Long> cachedNodes =
                    cachedNodesMap.get( valueAsString );
                if ( cachedNodes != null )
                {
                    foundInCache = true;
                    for ( Long cachedNodeId : cachedNodes )
                    {
                        nodes.add( getNeo().getNodeById( cachedNodeId ) );
                    }
                }
            }
            if ( !foundInCache )
            {
                Iterable<Node> readNodes = searchForNodes( key, value );
                ArrayList<Long> readNodeIds = new ArrayList<Long>();
                for ( Node readNode : readNodes )
                {
                    nodes.add( readNode );
                    readNodeIds.add( readNode.getId() );
                }
                if ( cachedNodesMap != null )
                {
                    cachedNodesMap.put( valueAsString, readNodeIds );
                }
            }
        }
        return nodes;
    }
    
    public void setSorting( Sort sortingOrNullForNone )
    {
        this.sorting = sortingOrNullForNone;
    }
    
    protected Query formQuery( String key, Object value )
    {
        return new TermQuery( new Term( DOC_INDEX_KEY, value.toString() ) );
    }

    private Iterable<Node> searchForNodes( String key, Object value )
    {
        Query query = formQuery( key, value );
        try
        {
            IndexSearcher searcher = xaDs.getIndexSearcher( key );
            ArrayList<Node> nodes = new ArrayList<Node>();
            Hits hits = sorting != null ?
                searcher.search( query, sorting ) :
                searcher.search( query );
            for ( int i = 0; i < hits.length(); i++ )
            {
                Document document = hits.doc( i );
                try
                {
                    long id = Long.parseLong( document.getField( DOC_ID_KEY )
                        .stringValue() );
                    nodes.add( getNeo().getNodeById( id ) );
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
    }

    public Node getSingleNode( String key, Object value )
    {
        // TODO Temporary code, this can be implemented better
        Iterator<Node> nodes = getNodes( key, value ).iterator();
        Node node = nodes.hasNext() ? nodes.next() : null;
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

    @Override
    protected void removeIndexThisTx( Node node, String key, Object value )
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
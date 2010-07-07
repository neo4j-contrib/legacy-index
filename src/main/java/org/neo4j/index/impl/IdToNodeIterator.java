package org.neo4j.index.impl;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.PrefetchingIterator;

/**
 * Converts an Iterator<Long> of node ids to an Iterator<Node> where the
 * {@link GraphDatabaseService#getNodeById(long)} is used to look up the nodes,
 * one call per step in the iterator.
 */
public class IdToNodeIterator extends PrefetchingIterator<Node>
{
    private final Iterator<Long> ids;
    private final GraphDatabaseService graphDb;
    
    /**
     * @param ids the node ids to use as underlying iterator.
     * @param graphDb the {@link GraphDatabaseService} to use for node lookups.
     */
    public IdToNodeIterator( Iterator<Long> ids, GraphDatabaseService graphDb )
    {
        this.ids = ids;
        this.graphDb = graphDb;
    }
    
    @Override
    protected Node fetchNextOrNull()
    {
        Node result = null;
        while ( result == null )
        {
            if ( !ids.hasNext() )
            {
                return null;
            }
            
            long id = ids.next();
            try
            {
                return graphDb.getNodeById( id );
            }
            catch ( NotFoundException e )
            {
                // Rare exception which can occur under normal
                // circumstances
            }
        }
        return result;
    }
}

package org.neo4j.index.impl;

import java.util.Iterator;

import org.neo4j.commons.iterator.PrefetchingIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;

public class IdToNodeIterator extends PrefetchingIterator<Node>
{
    private final Iterator<Long> ids;
    private final GraphDatabaseService neo;
    
    public IdToNodeIterator( Iterator<Long> ids, GraphDatabaseService neo )
    {
        this.ids = ids;
        this.neo = neo;
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
                return neo.getNodeById( id );
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

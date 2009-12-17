package org.neo4j.util.index;

import java.util.Iterator;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.commons.iterator.PrefetchingIterator;

class IdToNodeIterator extends PrefetchingIterator<Node>
{
    private final Iterator<Long> ids;
    private final NeoService neo;
    
    IdToNodeIterator( Iterator<Long> ids, NeoService neo )
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

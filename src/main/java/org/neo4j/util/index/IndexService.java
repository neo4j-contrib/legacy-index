package org.neo4j.util.index;

import org.neo4j.api.core.Node;

public interface IndexService
{
    void index( Node node, String key, Object value );

    Node getSingleNode( String key, Object value );

    Iterable<Node> getNodes( String key, Object value );

    void removeIndex( Node node, String key, Object value );
    
    void setIsolation( Isolation level );
}

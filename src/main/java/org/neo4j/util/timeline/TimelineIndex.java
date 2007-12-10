package org.neo4j.util.timeline;

import org.neo4j.api.core.Node;

public interface TimelineIndex
{
    public Node getLastNode();
    
    public Node getFirstNode();
    
    public void removeNode( Node nodeToRemove );
    
    public void addNode( Node nodeToAdd, long timestamp );
    
    public Iterable<Node> getNodes( long timestamp );
    
    public Iterable<Node> getAllNodes();
    
    public Iterable<Node> getAllNodesAfter( long timestamp );

    public Iterable<Node> getAllNodesBefore( long timestamp );
    
    public Iterable<Node> getAllNodesBetween( long startTime, 
        long endTime );
    
    public void delete();
}

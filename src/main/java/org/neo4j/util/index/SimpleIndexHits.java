package org.neo4j.util.index;

import java.util.Iterator;

import org.neo4j.api.core.Node;

public class SimpleIndexHits implements IndexHits
{
    private final Iterable<Node> nodes;
    private final int size;
    
    public SimpleIndexHits( Iterable<Node> nodes, int size )
    {
        this.nodes = nodes;
        this.size = size;
    }
    
    public Iterator<Node> iterator()
    {
        return this.nodes.iterator();
    }

    public int size()
    {
        return this.size;
    }
}

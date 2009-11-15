package org.neo4j.util.index;

import java.util.Iterator;

public class SimpleIndexHits<T> implements IndexHits<T>
{
    private final Iterable<T> hits;
    private final int size;
    
    public SimpleIndexHits( Iterable<T> hits, int size )
    {
        this.hits = hits;
        this.size = size;
    }
    
    public Iterator<T> iterator()
    {
        return this.hits.iterator();
    }

    public int size()
    {
        return this.size;
    }
}

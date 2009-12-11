package org.neo4j.util.index;

import java.util.Iterator;

/**
 * A simple implementation of an {@link IndexHits} where the size is known at
 * construction time.
 *
 * @param <T> the type of items.
 */
public class SimpleIndexHits<T> implements IndexHits<T>
{
    private final Iterable<T> hits;
    private final int size;
    
    /**
     * Wraps an Iterable<T> with a known size.
     * 
     * @param hits the hits to iterate through.
     * @param size the size of the iteration.
     */
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

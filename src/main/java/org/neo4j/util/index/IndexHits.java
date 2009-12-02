package org.neo4j.util.index;

/**
 * It's just an Iterable<T> which has a {@link #size()} method on it.
 * Ideally the size is calculated in some other (more optimized) way than
 * looping through all the items in the iterator so it's ok using any way you
 * like.
 * 
 * @param <T> the type of items in the Iterable.
 */
public interface IndexHits<T> extends Iterable<T>
{
    /**
     * @return the size of this iterable. Ideally the size is given at
     * construction time so that the size is known before-hand. This method
     * should _not_ be implemented as looping through all the items.
     */
    int size();
}

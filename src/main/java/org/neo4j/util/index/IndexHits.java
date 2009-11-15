package org.neo4j.util.index;

public interface IndexHits<T> extends Iterable<T>
{
    int size();
}

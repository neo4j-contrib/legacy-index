package org.neo4j.util.index;

import java.util.Iterator;

class LazyIndexHits<T> implements IndexHits<T>
{
    private final IndexHits<T> hits;
    private final IndexSearcherRef searcher;
    
    LazyIndexHits( IndexHits<T> hits, IndexSearcherRef searcher )
    {
        this.hits = hits;
        this.searcher = searcher;
    }

    public void close()
    {
        this.hits.close();
        if ( this.searcher != null )
        {
            this.searcher.closeStrict();
        }
    }

    public int size()
    {
        return this.hits.size();
    }

    public Iterator<T> iterator()
    {
        return this.hits.iterator();
    }
}

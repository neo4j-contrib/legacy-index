package org.neo4j.util.index;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.IndexSearcher;

class IndexSearcherRef
{
    private final String key;
    private final IndexSearcher searcher;
    private final AtomicInteger refCount = new AtomicInteger( 1 );
    private boolean isClosed;
    private boolean detached;
    
    public IndexSearcherRef( String key, IndexSearcher searcher )
    {
        this.key = key;
        this.searcher = searcher;
    }
    
    IndexSearcher getSearcher()
    {
        return this.searcher;
    }
    
    String getKey()
    {
        return this.key;
    }
    
    void incRef()
    {
        this.refCount.incrementAndGet();
    }
    
    void detachOrClose() throws IOException
    {
        if ( this.refCount.get() == 0 )
        {
            close();
        }
        else
        {
            this.detached = true;
        }
    }
    
    boolean close() throws IOException
    {
        if ( this.isClosed )
        {
            return true;
        }
        
        boolean reallyClosed = false;
        if ( this.refCount.decrementAndGet() == 0 && this.detached )
        {
            this.searcher.close();
            this.searcher.getIndexReader().close();
            reallyClosed = true;
            this.isClosed = true;
            new Exception( "closed searcher '" +
                this.key + "'" ).printStackTrace();
        }
        return reallyClosed;
    }
    
    boolean closeStrict()
    {
        try
        {
            return close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}

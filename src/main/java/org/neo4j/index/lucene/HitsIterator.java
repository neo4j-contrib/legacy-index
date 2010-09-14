package org.neo4j.index.lucene;

import java.io.IOException;

import org.apache.lucene.Hits;
import org.apache.lucene.document.Document;
import org.neo4j.helpers.collection.PrefetchingIterator;

class HitsIterator extends PrefetchingIterator<Document>
{
    private final Hits hits;
    private final int size;
    private int index;
    
    HitsIterator( Hits hits )
    {
        this.hits = hits;
        this.size = hits.length();
    }

    @Override
    protected Document fetchNextOrNull()
    {
        int i = index++;
        try
        {
            return i < size ? hits.doc( i ) : null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public int size()
    {
        return this.size;
    }
}

package org.neo4j.util.index;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;

abstract class GenericIndexService implements IndexService
{
    private final NeoService neo;
    private final IndexServiceQueue queue;
    
    private ThreadLocal<Isolation> threadIsolation =
        new ThreadLocal<Isolation>()
        {
            @Override
            protected Isolation initialValue()
            {
                return Isolation.SAME_TX;
            }
        };
    
    protected abstract void removeIndexThisTx( Node node, String key, 
        Object value );

    protected abstract void indexThisTx( Node node, String key, Object value );

    public GenericIndexService( NeoService service )
    {
        if ( service == null )
        {
            throw new IllegalArgumentException( "Null neo service" );
        }
        this.neo = service;
        queue = new IndexServiceQueue( this );
        queue.start();
    }
    
    public void index( Node node, String key, Object value )
    {
        Isolation level = threadIsolation.get();
        if ( level == Isolation.SAME_TX )
        {
            indexThisTx( node, key, value );
        }
        else
        {
            queue.queueIndex( level, node, key, value );
        }
    }
    
    public void removeIndex( Node node, String key, Object value )
    {
        Isolation level = threadIsolation.get();
        if ( level == Isolation.SAME_TX )
        {
            removeIndexThisTx( node, key, value );
        }
        else
        {
            queue.queueRemove( level, node, key, value );
        }
    }
    
    protected NeoService getNeo()
    {
        return neo;
    }
    
    public void setIsolation( Isolation level )
    {
        threadIsolation.set( level );
    }
    
    public Isolation getIsolation()
    {
        return threadIsolation.get();
    }
    
    protected Transaction beginTx()
    {
        return neo.beginTx();
    }
    
    public void shutdown()
    {
        queue.stopRunning();
    }
    
    protected IndexServiceQueue getQueue()
    {
        return queue;
    }
}

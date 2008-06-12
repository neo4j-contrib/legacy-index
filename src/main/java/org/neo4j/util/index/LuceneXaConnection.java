package org.neo4j.util.index;

import java.util.Set;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import org.neo4j.api.core.Node;
import org.neo4j.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.impl.transaction.xaframework.XaResourceManager;

class LuceneXaConnection extends XaConnectionHelpImpl
{
    private final LuceneXaResource xaResource;
    
    LuceneXaConnection( Object identifier, XaResourceManager xaRm )
    {
        super( xaRm );
        xaResource = new LuceneXaResource( identifier, xaRm );
    }
    
    @Override
    public XAResource getXaResource()
    {
        return xaResource;
    }
    
    private static class LuceneXaResource extends XaResourceHelpImpl
    {
        private final Object identifier;
        
        LuceneXaResource( Object identifier, XaResourceManager xaRm )
        {
            super( xaRm );
            this.identifier = identifier;
        }
        
        @Override
        public boolean isSameRM( XAResource xares )
        {
            if ( xares instanceof LuceneXaResource )
            {
                return identifier.equals( 
                    ((LuceneXaResource) xares).identifier );
            }
            return false;
        }
    }

    private LuceneTransaction luceneTx;
    
    LuceneTransaction getLuceneTx()
    {
        if ( luceneTx == null )
        {
            try
            {
                luceneTx = ( LuceneTransaction ) getTransaction();
            }
            catch ( XAException e )
            {
                throw new RuntimeException( "Unable to get lucene tx", e );
            }
        }
        return luceneTx;
    }
    
    public void index( Node node, String key, Object value )
    {
        getLuceneTx().index( node, key, value );
    }
    
    public void removeIndex( Node node, String key, Object value )
    {
        getLuceneTx().removeIndex( node, key, value );
    }
    
    public Set<Long> getDeletedNodesFor( String key, Object value )
    {
        return getLuceneTx().getDeletedNodesFor( key, value );
    }
    
    public Set<Long> getAddedNodesFor( String key, Object value )
    {
        return getLuceneTx().getNodesFor( key, value );
    }
}

package org.neo4j.util.index;

import java.util.Collections;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.util.ArrayMap;

public class NeoIndexService implements IndexService
{
    private final NeoService neo;
    private final Node rootIndexService;
    private final Index keyToIndex;
    private final ArrayMap<String,Index> keyToIndexCache = 
        new ArrayMap<String,Index>( 6, true, true );
    private final NeoIndexServiceQueue queue;
    
    
    private ThreadLocal<Isolation> threadIsolation =
        new ThreadLocal<Isolation>()
        {
    		@Override
            protected Isolation initialValue()
            {
                return Isolation.SAME_TX;
            }
        };
    
    private enum RelTypes implements RelationshipType
    {
        INDEX_SERVICE,
        KEY_INDEX,
        VALUE_INDEX,
    }

    public NeoIndexService( NeoService service )
    {
        if ( service == null )
        {
            throw new IllegalArgumentException( "Null neo service" );
        }
        this.neo = service;
        Transaction tx = service.beginTx();
        try
        {
            Node refNode = service.getReferenceNode();
            Relationship rel = refNode.getSingleRelationship( 
                RelTypes.INDEX_SERVICE, Direction.OUTGOING );
            if ( rel == null )
            {
                rootIndexService = neo.createNode();
                refNode.createRelationshipTo( rootIndexService, 
                    RelTypes.INDEX_SERVICE );
                Node keyIndexRoot = neo.createNode();
                rootIndexService.createRelationshipTo( keyIndexRoot, 
                    RelTypes.KEY_INDEX );
                keyToIndex = new SingleValueIndex( "keyToIndex", keyIndexRoot, 
                    neo );
            }
            else
            {
                rootIndexService = rel.getEndNode();
                Relationship keyRel = rootIndexService.getSingleRelationship( 
                    RelTypes.KEY_INDEX, Direction.OUTGOING );
                if ( keyRel == null )
                {
                    throw new RuntimeException( "Unable to locate KeyToIndex " +
                        "relationship in index service." );
                }
                Node keyIndexRoot = keyRel.getEndNode();
                keyToIndex = new SingleValueIndex( "keyToIndex", keyIndexRoot, 
                    neo );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
            queue = new NeoIndexServiceQueue( this );
            queue.start();
        }
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
//        else
//        {
//            throw new IllegalStateException( "Unkown isolation: " + level );
//        }
    }
    
    void indexThisTx( Node node, String key, Object value )
    {
        // get the value index
        Index valueIndex = getValueIndex( key, true );
        valueIndex.index( node, value );
    }
    
    Index getValueIndex( String key, boolean create )
    {
        Index valueIndex = keyToIndexCache.get( key );
        if ( valueIndex == null )
        {
            Node valueIndexNode = keyToIndex.getSingleNodeFor( key );
            if ( valueIndexNode == null && create )
            {
                // create new value index
                valueIndexNode = neo.createNode();
                rootIndexService.createRelationshipTo( valueIndexNode, 
                    RelTypes.VALUE_INDEX );
                valueIndex = new MultiValueIndex( "index_" + key, 
                    valueIndexNode, neo );
                keyToIndexCache.put( key, valueIndex );
            }
//            else if ( !create )
//            {
//                valueIndex = new MultiValueIndex( "index_" + key, 
//                    valueIndexNode, neo );
//                keyToIndexCache.put( key, valueIndex );
//            }
        }
        return valueIndex;
    }

    public Iterable<Node> getNodes( String key, Object value )
    {
        Index valueIndex = getValueIndex( key, false );
        if ( valueIndex == null )
        {
            return Collections.EMPTY_LIST;
        }
        return valueIndex.getNodesFor( value );
    }

    public Node getSingleNode( String key, Object value )
    {
        Index valueIndex = getValueIndex( key, false );
        if ( valueIndex == null )
        {
            return null;
        }
        return valueIndex.getSingleNodeFor( value );
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
//        else
//        {
//            throw new IllegalStateException( "Unkown isolation: " + level );
//        }
    }
    
    void removeIndexThisTx( Node node, String key, Object value )
    {
        Index valueIndex = getValueIndex( key, false );
        if ( valueIndex != null )
        {
            valueIndex.remove( node, value );
        }
    }

    public void setIsolation( Isolation level )
    {
        threadIsolation.set( level );
    }
    
    Transaction beginTx()
    {
        return neo.beginTx();
    }
    
    public void shutdown()
    {
        queue.stopRunning();
    }
}
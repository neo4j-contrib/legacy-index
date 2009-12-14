/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.util.index;

import java.util.Collections;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.util.ArrayMap;

/**
 * An {@link IndexService} implemented with {@link SingleValueIndex}, one for
 * each key.
 * 
 * This class isn't ready for general usage yet and use of it is discouraged.
 * 
 * @deprecated
 */
public class NeoIndexService extends GenericIndexService
{
    private final Node rootIndexService;
    private final Index keyToIndex;
    private final ArrayMap<String,Index> keyToIndexCache = 
        new ArrayMap<String,Index>( 6, true, true );
    
    
    private enum RelTypes implements RelationshipType
    {
        INDEX_SERVICE,
        KEY_INDEX,
        VALUE_INDEX,
    }

    /**
     * @param neo the {@link NeoService} to use.
     */
    public NeoIndexService( NeoService neo )
    {
        super( neo );
        Transaction tx = neo.beginTx();
        try
        {
            Node refNode = neo.getReferenceNode();
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
        }
    }
    
    @Override
    protected void indexThisTx( Node node, String key, Object value )
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
                valueIndexNode = getNeo().createNode();
                keyToIndex.index( valueIndexNode, key );
                rootIndexService.createRelationshipTo( valueIndexNode, 
                    RelTypes.VALUE_INDEX );
                valueIndex = new MultiValueIndex( "index_" + key, 
                    valueIndexNode, getNeo() );
                keyToIndexCache.put( key, valueIndex );
            }
            else if ( valueIndexNode != null )
            {
                valueIndex = new MultiValueIndex( "index_" + key, 
                    valueIndexNode, getNeo() );
                keyToIndexCache.put( key, valueIndex );
            }
        }
        return valueIndex;
    }

    public IndexHits<Node> getNodes( String key, Object value )
    {
        Index valueIndex = getValueIndex( key, false );
        if ( valueIndex == null )
        {
            return new SimpleIndexHits<Node>(
                Collections.<Node>emptyList(), 0 );
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

    @Override
    protected void removeIndexThisTx( Node node, String key, Object value )
    {
        Index valueIndex = getValueIndex( key, false );
        if ( valueIndex != null )
        {
            valueIndex.remove( node, value );
        }
    }
}
/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.index.impl;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;

/**
 * Basic implementation of an {@link IndexService} with common functionality.
 */
public abstract class GenericIndexService implements IndexService
{
    private final GraphDatabaseService graphDb;
    
    protected abstract void removeIndexThisTx( Node node, String key, 
        Object value );

    protected abstract void indexThisTx( Node node, String key, Object value );

    /**
     * @param graphDb the {@link GraphDatabaseService} to associate this index
     * to.
     */
    public GenericIndexService( GraphDatabaseService graphDb )
    {
        if ( graphDb == null )
        {
            throw new IllegalArgumentException( "Null graph database service" );
        }
        this.graphDb = graphDb;
    }
    
    public void index( Node node, String key, Object value )
    {
        indexThisTx( node, key, value );
    }
    
    public void removeIndex( Node node, String key, Object value )
    {
        removeIndexThisTx( node, key, value );
    }
    
    protected GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }
    
    protected Transaction beginTx()
    {
        return graphDb.beginTx();
    }
    
    public void shutdown()
    {
    }
}

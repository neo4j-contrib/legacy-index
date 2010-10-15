/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.PrefetchingIterator;

/**
 * Converts an Iterator<Long> of node ids to an Iterator<Node> where the
 * {@link GraphDatabaseService#getNodeById(long)} is used to look up the nodes,
 * one call per step in the iterator.
 */
public class IdToNodeIterator extends PrefetchingIterator<Node>
{
    private final Iterator<Long> ids;
    private final GraphDatabaseService graphDb;
    
    /**
     * @param ids the node ids to use as underlying iterator.
     * @param graphDb the {@link GraphDatabaseService} to use for node lookups.
     */
    public IdToNodeIterator( Iterator<Long> ids, GraphDatabaseService graphDb )
    {
        this.ids = ids;
        this.graphDb = graphDb;
    }
    
    @Override
    protected Node fetchNextOrNull()
    {
        Node result = null;
        while ( result == null )
        {
            if ( !ids.hasNext() )
            {
                return null;
            }
            
            long id = ids.next();
            try
            {
                return graphDb.getNodeById( id );
            }
            catch ( NotFoundException e )
            {
                // Rare exception which can occur under normal
                // circumstances
            }
        }
        return result;
    }
}

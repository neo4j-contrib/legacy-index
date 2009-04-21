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

import org.neo4j.api.core.Node;

/**
 * An index service can be used to index nodes with a key and a value. 
 */
public interface IndexService
{
    /**
     * Index <code>node</code> with <code>key</code> and <code>value</code>.
     * 
     * @param node node to index
     * @param key key for the index
     * @param value value to index the node with
     */
    void index( Node node, String key, Object value );

    /**
     * Returns a single node indexed with <code>key</code> and 
     * <code>value</code>. If no such index exist <code>null</code> is 
     * returned. If more then one node is found a runtime exception is 
     * thrown.
     * 
     * @param key key for index
     * @param value value for index
     * @return node that has been indexed with key and value or 
     * <code>null</code>
     */
    Node getSingleNode( String key, Object value );

    /**
     * Returns all nodes indexed with <code>key</code> and 
     * <code>value</code>.
     * 
     * @param key key for index
     * @param value value for index
     * @return nodes that has been indexed with key and value
     */
    Iterable<Node> getNodes( String key, Object value );

    /**
     * Removes a index for a node. If no such indexing exist this method 
     * silently returns.
     * 
     * @param node node to remove indexing from
     * @param key key of index
     * @param value value of index
     */
    void removeIndex( Node node, String key, Object value );
    
    /**
     * Changes isolation level for the running transaction. This method must
     * be invoked before any index (add/remove) has been performed in the 
     * current transaction. Default isolation level is 
     * {@link Isolation#SAME_TX}.
     * 
     * @param level new isolation level
     */
    void setIsolation( Isolation level );

    /**
     * Stops this indexing service comitting any asynchronous requests that 
     * are currently queued. After this method has been invoked any following
     * method invoke on this instance is invalid.
     */
    void shutdown();
}

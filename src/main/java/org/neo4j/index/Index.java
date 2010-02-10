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
package org.neo4j.index;

import org.neo4j.graphdb.Node;

/**
 * An index that indexes nodes with a key.
 * 
 * This class isn't ready for general usage yet and use of it is discouraged.
 * 
 * @deprecated
 */
@Deprecated
public interface Index
{
    /**
     * Create a index mapping between a node and a key.
     * 
     * @param nodeToIndex the node to index
     * @param indexKey the key
     */
    public void index( Node nodeToIndex, Object indexKey );

    /**
     * Returns nodes indexed with <CODE>indexKey</CODE>
     * 
     * @param indexKey the index key
     * @return nodes mapped to <CODE>indexKey</CODE>
     */
    public IndexHits<Node> getNodesFor( Object indexKey );

    /**
     * Returns a single node indexed with <CODE>indexKey</CODE>. If more then
     * one node is indexed with that key a <CODE>RuntimeException</CODE> is
     * thrown. If no node is indexed with the key <CODE>null</CODE> is returned.
     * 
     * @param indexKey the index key
     * @return the single node mapped to <CODE>indexKey</CODE>
     */
    public Node getSingleNodeFor( Object indexKey );

    /**
     * Removes a index mapping between a node and a key.
     * 
     * @param nodeToRemove node to remove
     * @param indexKey the key
     */
    public void remove( Node nodeToRemove, Object indexKey );

    /**
     * Deletes this index.
     */
    public void drop();

    /**
     * Deletes this index using a commit interval.
     * 
     * @param commitInterval number of index mappings removed before a new
     *            transaction is started
     */
    public void drop( int commitInterval );

    /**
     * Removes all the entries from this index.
     */
    public void clear();

    /**
     * Returns all nodes in this index. Same node may be returned many times
     * depending on implementation.
     * 
     * @return all nodes in this index
     * @throws UnsupportedOperationException if the <CODE>values()</CODE> method
     *             isn't supported by this index.
     */
    public Iterable<Node> values();
}

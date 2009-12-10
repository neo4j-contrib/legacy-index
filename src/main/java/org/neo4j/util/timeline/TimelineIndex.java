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
package org.neo4j.util.timeline;

import org.neo4j.api.core.Node;

/**
 * An utility for ordering nodes in a timeline. You add nodes to the timeline
 * and then you can query for nodes given a time period, w/ or w/o lower/upper
 * bounds, f.ex. "Give me all nodes before this given timestamp" or
 * "Give me all nodes between these two timestamps".
 */
public interface TimelineIndex
{
    /**
     * @return the last node in the timeline or {@code null} if timeline
     * is empty.
     */
    Node getLastNode();
    
    /**
     * @return the first node in the timeline or {@code null} if timeline
     * is empty.
     */
    Node getFirstNode();
    
    /**
     * Removes a node from the timeline. 
     * 
     * @param nodeToRemove the node to remove from this timeline
     * @throws IllegalArgumentException if {@code null} node or node not 
     * connected to this timeline.
     */
    void removeNode( Node nodeToRemove );
    
    /**
     * Adds a node in the timeline with the given {@code timestamp}.
     * 
     * @param nodeToAdd the node to add to this timeline.
     * @param timestamp the timestamp to use
     * @throws IllegalArgumentException If already added to this timeline or or 
     * <CODE>null</CODE> node
     */
    void addNode( Node nodeToAdd, long timestamp );
    
    /**
     * @param timestamp the timestamp to get nodes for.
     * @return nodes which were added with the given {@code timestamp}.
     */
    Iterable<Node> getNodes( long timestamp );
    
    /**
     * @return all the nodes in the timeline ordered by increasing timestamp.
     */
    Iterable<Node> getAllNodes();
    
    /**
     * @param timestamp the timestamp value, nodes with greater timestamp 
     * value will be returned.
     * @return all nodes after (exclusive) the specified timestamp 
     * ordered by increasing timestamp.
     */
    Iterable<Node> getAllNodesAfter( long timestamp );

    /**
     * @param timestamp the timestamp value, nodes with lesser timestamp 
     * value will be returned.
     * @return all nodes before (exclusive) the specified timestamp 
     * ordered by increasing timestamp.
     */
    Iterable<Node> getAllNodesBefore( long timestamp );
    
    /**
     * @param startTimestamp the start timestamp, nodes with greater timestamp 
     * value will be returned.
     * @param endTimestamp the end timestamp, nodes with lesser timestamp 
     * value will be returned.
     * @return all nodes between (exclusive) the specified timestamps 
     * ordered by increasing timestamp.
     */
    Iterable<Node> getAllNodesBetween( long startTimestamp, 
        long endTimestamp );
    
    /**
     * Convenience method which you can use {@link #getAllNodes()},
     * {@link #getAllNodesAfter(long)}, {@link #getAllNodesBefore(long)} and
     * {@link #getAllNodesBetween(long, long)} in a single method.
     * 
     * @param startTimestampOrNull the start timestamp, nodes with greater
     * timestamp value will be returned. Will be ignored if {@code null}.
     * @param endTimestampOrNull the end timestamp, nodes with lesser
     * timestamp value will be returned. Will be ignored if {@code null}.
     * @return all nodes in this timeline ordered by timestamp. A range can be
     * given with the {@code startTimestampOrNull} and/or
     * {@code endTimestampOrNull} (where {@code null} means no restriction).
     */
    Iterable<Node> getAllNodes( Long startTimestampOrNull,
        Long endTimestampOrNull );
        
    /**
     * Deletes this timeline. Nodes added to the timeline will not be deleted, 
     * they are just disconnected from this timeline.
     */
    void delete();
}

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
 * An interface for representing a timeline.
 */
public interface TimelineIndex
{
    /**
     * Return the last node in the timeline or <CODE>null</CODE> if timeline
     * is empty.
     * 
     * @return The last node in the timeline
     */
    Node getLastNode();
    
    /**
     * Returns the first node in the timeline or <CODE>null</CODE> if timeline
     * is empty.
     * 
     * @return The first node in the timeline
     */
    Node getFirstNode();
    
    /**
     * Removes a node from the timeline. 
     * 
     * @param nodeToRemove The node to remove from this timeline
     * @throws IllegalArgumentException if <CODE>null</CODE> node or node not 
     * connected to this timeline.
     */
    void removeNode( Node nodeToRemove );
    
    /**
     * Adds a node in the timeline using <CODE>timestamp</CODE>.
     * 
     * @param nodeToAdd The node to add to the timeline
     * @param timestamp The timestamp to use
     * @throws IllegalArgumentException If already added to this timeline or or 
     * <CODE>null</CODE> node
     */
    void addNode( Node nodeToAdd, long timestamp );
    
    /**
     * @param timestamp the timestamp to get nodes for.
     * @return nodes for a given timestamp.
     */
    Iterable<Node> getNodes( long timestamp );
    
    /**
     * Returns all nodes in the timeline ordered by increasing timestamp.
     * 
     * @return All nodes in the timeline
     */
    Iterable<Node> getAllNodes();
    
    /**
     * Returns all nodes after (not including) the specified timestamp 
     * ordered by increasing timestamp.
     * 
     * @param timestamp The timestamp value, nodes with greater timestamp 
     * value will be returned
     * @return All nodes in the timeline after specified timestamp
     */
    Iterable<Node> getAllNodesAfter( long timestamp );

    /**
     * Returns all nodes before (not including) the specified timestamp 
     * ordered by increasing timestamp.
     * 
     * @param timestamp The timestamp value, nodes with lesser timestamp 
     * value will be returned
     * @return All nodes in the timeline after specified timestamp
     */
    Iterable<Node> getAllNodesBefore( long timestamp );
    
    /**
     * Returns all nodes between (not including) the specified timestamps 
     * ordered by increasing timestamp.
     * 
     * @param startTime The start timestamp, nodes with greater timestamp 
     * value will be returned
     * @param endTime The end timestamp, nodes with lesser timestamp 
     * value will be returned
     * @return All nodes in the timeline between the specified timestamps
     */
    Iterable<Node> getAllNodesBetween( long startTime, 
        long endTime );
    
    /**
     * Convenience method which is rather flexible with how the range is
     * selected based on what you pass in.
     * 
     * @param afterTimestampOrNull passed in as a range restriction so that
     * only nodes after this given timestamp are returned. Can be combine with
     * {@code beforeTimestampOrNull}.
     * @param beforeTimestampOrNull passed in as a range restriction so that
     * only nodes before this given timestamp are returned. Can be combined with
     * {@code afterTimestampOrNull}}
     * @return nodes in this timeline in order of timestamps. A range can be
     * given with the {@code afterTimestampOrNull} and
     * {@code beforeTimestampOrNull} (where {@code null} means no restriction).
     */
    Iterable<Node> getAllNodes( Long afterTimestampOrNull,
        Long beforeTimestampOrNull );
        
    /**
     * Deletes this timeline. Nodes added to the timeline will not be deleted, 
     * they are just disconnected from this timeline.
     */
    void delete();
}

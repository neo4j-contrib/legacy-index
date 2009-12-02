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

/**
 * The isolation level of the indexing, f.ex. do we do the actual indexing
 * right now and in the same transaction? Or do we put it in a queue and
 * perform it at a later time?
 */
public enum Isolation
{
    /**
     * Happens right now and in the same transaction.
     */
    SAME_TX,
    
    /**
     * Happens right now, but in its own transaction.
     */
    SYNC_OTHER_TX,
    
    /**
     * The actual indexing will happen in the future and is typically put
     * in a queue for indexing later on.
     */
    ASYNC_OTHER_TX
}

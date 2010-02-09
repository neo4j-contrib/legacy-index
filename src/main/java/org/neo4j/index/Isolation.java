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

/**
 * The isolation level of the indexing. F.ex. should the actual indexing be done
 * right now and in the same transaction? Or should it be put in a queue and
 * performed it at a later time?
 * 
 * See more information at <a href=
 * "http://wiki.neo4j.org/content/Indexing_with_IndexService#Isolation_levels">
 * The Isolation levels section of the Neo4j wiki page on
 * "Indexing with IndexService"</a>.
 */
public enum Isolation
{
    /**
     * Happens right now and in the same transaction. Pros:
     * <ul>
     * <li>You have control that the indexing happens in your current
     * transaction and the changes will be gracefully rolled back if an error
     * should occur in the middle of your transaction</li>
     * <li>You will get errors in the executing thread, making it easier to
     * detect and handle errors.
     * </ul>
     * Cons:
     * <ul>
     * <li>It can potentially affect performance since the indexing happens
     * synchronously.
     * </ul>
     */
    SAME_TX,

    /**
     * Happens right now, but in its own transaction. Pros:
     * <ul>
     * <li>You will get errors in the executing thread, making it easier to
     * detect and handle errors</li>
     * <li>If you're doing some things which specifically require the indexing
     * to be executed in its own separate transaction (yet same thread) than the
     * one the calling thread is in, this can be done with this isolation.</li>
     * </ul>
     * Cons:
     * <ul>
     * <li>It can potentially affect performance since the indexing happens
     * synchronously (in its own separate transaction as well)..
     * </ul>
     */
    SYNC_OTHER_TX,

    /**
     * The actual indexing will happen in the future and in another thread. The
     * job is typically put in a queue for indexing later on. Pros:
     * <ul>
     * <li>Won't affect the calling thread's performance since just putting the
     * job on the queue is very fast.</li>
     * </ul>
     * Cons:
     * <ul>
     * <li>Detecting and handling errors during actual indexing gets harder and
     * some degree of control is lost.</li>
     * <li>Whether or not the indexing queue is persistent is implementation
     * specific so there's a potential risk of losing indexing jobs if the JVM
     * or IndexService is shutdown while there's more jobs in the indexing
     * queue.
     * </ul>
     */
    ASYNC_OTHER_TX
}

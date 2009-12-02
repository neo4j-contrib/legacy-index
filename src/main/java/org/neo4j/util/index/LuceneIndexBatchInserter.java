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

import org.apache.lucene.index.IndexWriter;
import org.neo4j.impl.batchinsert.BatchInserter;

/**
 * The "batch inserter" version of a {@link LuceneIndexService}.
 */
public interface LuceneIndexBatchInserter
{
    /**
     * Adds an entry to the index.
     * 
     * @param node the node to associate with the {@code value} in the index
     * with name {@code key}.
     * @param key which index to put it in.
     * @param value the value to associate {@code node} with.
     */
    void index( long node, String key, Object value );
    
    /**
     * Shuts down this index.
     */
    void shutdown();

    /**
     * Gets nodes from the lucene index.
     * 
     * @param key the index.
     * @param value the value to query for.
     * @return the hits for the query.
     */
    IndexHits<Long> getNodes( String key, Object value );
    
    /**
     * Performs a Lucene optimize on the index files.
     * @see IndexWriter#optimize()
     */
    void optimize();

    /**
     * 
     * @param key 
     * @param value
     * @return the node id or -1 if no node found
     */
    long getSingleNode( String key, Object value );
    
    /**
     * @return this batch inserter index wrapped in a {@link IndexService}
     * interface for convenience. Goes well with an instance from
     * {@link BatchInserter#getNeoService()}.
     */
    IndexService getIndexService();
}
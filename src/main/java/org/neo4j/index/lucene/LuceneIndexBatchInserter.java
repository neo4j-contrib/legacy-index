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
package org.neo4j.index.lucene;

import org.apache.lucene.index.IndexWriter;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

/**
 * The "batch inserter" version of {@link LuceneIndexService}.
 * It should be used with a BatchInserter and stores the indexes in the same
 * format as {@link LuceneIndexService}.
 * 
 * It's optimized for large chunks of either reads or writes. So try to avoid
 * mixed reads and writes because there's a slight overhead to go from read mode
 * to write mode (the "mode" is per key and will not affect other keys)
 * 
 * See more information at http://wiki.neo4j.org/content/Indexing_with_BatchInserter
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
     * Shuts down this index and closes its underlying lucene index files.
     * If this isn't called before the JVM dies then there's no guarantee
     * that the indices are stored correctly on disk.
     */
    void shutdown();

    /**
     * Queries the lucene index for hits given the {@code key} and
     * {@code value}. The usage of {@link #getNodes(String, Object)},
     * {@link #getSingleNode(String, Object)} should be as separated as possible
     * from the {@link #index(long, String, Object)}. so that the performance
     * penalty gets as small as possible. Also see {@link #optimize()} for
     * separation between writes and reads.
     * 
     * @param key the index.
     * @param value the value to query for.
     * @return the hits for the query.
     */
    IndexHits<Long> getNodes( String key, Object value );
    
    /**
     * Performs a Lucene optimize on the index files. Do not use this too often
     * because it's a severe performance penalty. It optimizes the lucene
     * index files so that consecutive queries will be faster. So the optimal
     * usage is to index all of your stuff that you would like to index
     * (with minimal amount of reads during that time). When you're done
     * indexing and goes into a phase where you'd want to use the index for
     * lookups (maybe for creating relationships between nodes) you can call
     * this method so that your queries will be faster than they would otherwise
     * be.
     * 
     * @see IndexWriter#optimize()
     */
    void optimize();

    /**
     * @param key the index key to query.
     * @param value the value to query for.
     * @return the node id or -1 if no node found. It will throw a
     * RuntimeException if there's more than one node in the result.
     */
    long getSingleNode( String key, Object value );
    
    /**
     * @return this batch inserter index wrapped in a {@link IndexService}
     * interface for convenience. Goes well with an instance from
     * {@link BatchInserter#getNeoService()}.
     */
    IndexService getIndexService();
}
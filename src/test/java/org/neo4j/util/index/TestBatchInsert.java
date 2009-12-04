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

import java.io.File;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.neo4j.impl.batchinsert.BatchInserter;
import org.neo4j.impl.batchinsert.BatchInserterImpl;

public class TestBatchInsert extends TestCase
{
    @Override
    public void tearDown() throws Exception
    {
        deleteRecursivley( new File( "var/batch-insert") );
    }
    
    private void deleteRecursivley( File file )
    {
        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteRecursivley( child );
            }
        }
        file.delete();
    }
    
    public void testSimpleBatchInsert()
    {
        BatchInserter neo = new BatchInserterImpl( "var/batch-insert" );
        LuceneIndexBatchInserter index = 
            new LuceneIndexBatchInserterImpl( neo );
        try
        {
            long node = neo.createNode( null );
            assertTrue( !index.getNodes( "test-key", 
                "test-value" ).iterator().hasNext() );
            index.index( node, "test-key", "test-value" );
            assertTrue( index.getNodes( "test-key", 
                "test-value" ).iterator().hasNext() );
        }
        finally
        {
            index.shutdown();
            neo.shutdown();
        }
    }

    public void testSimpleFulltextBatchInsert()
    {
        BatchInserter neo = new BatchInserterImpl( "var/batch-insert" );
        LuceneIndexBatchInserter index = 
            new LuceneFulltextIndexBatchInserter( neo );
        try
        {
            long node = neo.createNode( null );
            assertTrue( !index.getNodes( "test-key", 
                "test-value" ).iterator().hasNext() );
            index.index( node, "test-key", "test-value" );
            assertTrue( index.getNodes( "test-key", 
                "test-value" ).iterator().hasNext() );
            
            String value = "A decent value for indexing";
            String key = "my key";
            index.index( node, key, value );
            for ( String word : value.split( Pattern.quote( " " ) ) )
            {
                assertTrue( index.getNodes( key, word ).iterator().hasNext() );
            }
            assertFalse( index.getNodes( key,
                "abcdefghijklmnop" ).iterator().hasNext() );
        }
        finally
        {
            index.shutdown();
            neo.shutdown();
        }
    }
    
    public void testMoreFulltextBatchInsert()
    {
        BatchInserter neo = new BatchInserterImpl( "var/batch-insert" );
        LuceneIndexBatchInserter index = 
            new LuceneFulltextIndexBatchInserter( neo );
        try
        {
            for ( int i = 0; i < 10000; i++ )
            {
                index.index( i, "mykey", i );
                assertEquals( i, index.getSingleNode( "mykey", i ) );
            }
        }
        finally
        {
            index.shutdown();
            neo.shutdown();
        }
    }
}

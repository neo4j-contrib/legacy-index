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

import java.io.File;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class TestBatchInsert extends TestCase
{
    private String getDbPath()
    {
        return "target/var/batch-insert";
    }
    
    @Override
    public void tearDown() throws Exception
    {
        deleteRecursivley( new File( getDbPath() ) );
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
        BatchInserter inserter = new BatchInserterImpl( getDbPath() );
        LuceneIndexBatchInserter index = 
            new LuceneIndexBatchInserterImpl( inserter );
        try
        {
            long node = inserter.createNode( null );
            assertTrue( !index.getNodes( "test-key", 
                "test-value" ).iterator().hasNext() );
            index.index( node, "test-key", "test-value" );
            assertTrue( index.getNodes( "test-key", 
                "test-value" ).iterator().hasNext() );
        }
        finally
        {
            index.shutdown();
            inserter.shutdown();
        }
    }

    public void testSimpleFulltextBatchInsert()
    {
        BatchInserter inserter = new BatchInserterImpl( getDbPath() );
        LuceneIndexBatchInserter index = 
            new LuceneFulltextIndexBatchInserter( inserter );
        try
        {
            long node = inserter.createNode( null );
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
            inserter.shutdown();
        }
    }
    
    public void testMoreFulltextBatchInsert()
    {
        BatchInserter inserter = new BatchInserterImpl( getDbPath() );
        LuceneIndexBatchInserter index = 
            new LuceneFulltextIndexBatchInserter( inserter );
        try
        {
            // Should be quite slow, i.e. don't build your code like this :)
            long time = System.currentTimeMillis();
            for ( int i = 0; i < 1000; i++ )
            {
                index.index( i, "mykey1", i );
                assertEquals( i, index.getSingleNode( "mykey1", i ) );
            }
            long slowTime = System.currentTimeMillis() - time;
            
            // Should be much faster
            time = System.currentTimeMillis();
            for ( int i = 0; i < 1000; i++ )
            {
                index.index( i, "mykey2", i );
            }
            index.optimize();
            for ( int i = 0; i < 1000; i++ )
            {
                assertEquals( i, index.getSingleNode( "mykey2", i ) );
            }
            long fastTime = System.currentTimeMillis() - time;
            assertTrue( fastTime < slowTime / 5 );
        }
        finally
        {
            index.shutdown();
            inserter.shutdown();
        }
    }
    
    public void testHmm() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( getDbPath() );
        LuceneIndexBatchInserter index = 
            new LuceneIndexBatchInserterImpl( inserter );
        try
        {
            int titleIdStart = 0;
            for ( int i = 0; i < 100; i++ )
            {
                index.index( titleIdStart + i, "ID_TITLE", i );
                index.index( titleIdStart + i, "NLC", "something" );
                index.index( titleIdStart + i, "TITLE", "title" );
                index.index( titleIdStart + i, "TYPE", "type" );
            }
    
            int imageIdStart = 1000;
            for ( int i = 0; i < 100; i++ )
            {
                index.index( imageIdStart + i, "ID_IMAGE", i );
                index.index( imageIdStart + i, "NLC", "something" );
                index.index( imageIdStart + i, "FILENAME", "filename" + i );
                index.index( imageIdStart + i, "TYPE", "type" );
            }
            index.optimize();
            
            for ( int i = 0; i < 100; i++ )
            {
                long tail = index.getSingleNode( "ID_TITLE", i );
                long head = index.getSingleNode( "ID_IMAGE", i );
                assertTrue( "tail -1", tail != -1 );
                assertTrue( "head -1", head != -1 );
            }
        }
        finally
        {
            index.shutdown();
            inserter.shutdown();
        }
    }
}

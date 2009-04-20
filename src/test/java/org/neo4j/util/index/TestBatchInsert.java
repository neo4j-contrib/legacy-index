package org.neo4j.util.index;

import java.io.File;

import org.neo4j.impl.batchinsert.BatchInserter;
import org.neo4j.util.NeoTestCase;

public class TestBatchInsert extends NeoTestCase
{
    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        deleteRecursivley( new File( "var/batch-insert") );
    }
    
    private void deleteRecursivley( File file )
    {
        if ( file.isDirectory() )
        {
            for ( String child : file.list() )
            {
                deleteRecursivley( new File( child ) );
            }
        }
        file.delete();
    }
    
    public void testSimpleBatchInsert()
    {
        BatchInserter neo = new BatchInserter( "var/batch-insert" );
        LuceneIndexBatchInserter index = new LuceneIndexBatchInserter( neo );
        
        long node = neo.createNode( null );
        assertTrue( !index.getNodes( "test-key", 
            "test-value" ).iterator().hasNext() );
        index.index( node, "test-key", "test-value" );
        assertTrue( index.getNodes( "test-key", 
        "test-value" ).iterator().hasNext() );
    }
}

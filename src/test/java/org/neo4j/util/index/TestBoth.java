package org.neo4j.util.index;

import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;

public class TestBoth extends NeoTestCase
{
    private IndexService indexService;
    private IndexService fulltextIndexService;
    
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        this.indexService = new LuceneIndexService( neo() );
        this.fulltextIndexService = new LuceneFulltextIndexService( neo() );
    }

    @Override
    protected void beforeNeoShutdown()
    {
        super.beforeNeoShutdown();
        this.indexService.shutdown();
        this.fulltextIndexService.shutdown();
    }
    
    public void testSome() throws Exception
    {
        Node node = neo().createNode();
        
        String key = "some_key";
        String value1 = "347384738-2";
        String value2 = "Tjena hej hoj";
        this.indexService.index( node, key, value1 );
        this.fulltextIndexService.index( node, key, value2 );
        
        restartTx();
        
        assertEquals( node, this.indexService.getSingleNode( key, value1 ) );
        assertEquals( node,
            this.fulltextIndexService.getSingleNode( key, "Tjena" ) );
        
        this.indexService.removeIndex( node, "cpv", value1 );
        this.fulltextIndexService.removeIndex( node, "cpv-ft", value2 );
        node.delete();
    }
}

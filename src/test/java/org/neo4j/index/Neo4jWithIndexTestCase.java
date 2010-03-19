package org.neo4j.index;

import org.junit.After;
import org.junit.Before;

public abstract class Neo4jWithIndexTestCase extends Neo4jTestCase
{
    private IndexService index;
    
    @Before
    public void setUpIndex()
    {
        index = instantiateIndex();
    }
    
    @After
    public void tearDownIndex()
    {
        finishTx( true );
        shutdownIndex();
    }
    
    protected void shutdownIndex()
    {
        index.shutdown();
    }

    @Override
    protected boolean manageMyOwnTxFinish()
    {
        return true;
    }
    
    protected abstract IndexService instantiateIndex();
    
    protected IndexService index()
    {
        return index;
    }
}

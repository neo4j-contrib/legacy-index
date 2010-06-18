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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexService;
import org.neo4j.index.Neo4jWithIndexTestCase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public class TestBothLuceneAndFulltext extends Neo4jWithIndexTestCase
{
    private IndexService fulltextIndexService;
    
    @Override
    protected IndexService instantiateIndex()
    {
        return new LuceneIndexService( graphDb() );
    }
    
    @Before
    public void setUpAdditionalFulltextIndex() throws Exception
    {
        this.fulltextIndexService = new LuceneFulltextIndexService( graphDb() );
    }

    @Override
    protected void shutdownIndex()
    {
        super.beforeShutdown();
        this.fulltextIndexService.shutdown();
    }
    
    @Test
    public void testSome() throws Exception
    {
        Node node = graphDb().createNode();
        
        String key = "some_key";
        String value1 = "347384738-2";
        String value2 = "Tjena hej hoj";
        index().index( node, key, value1 );
        this.fulltextIndexService.index( node, key, value2 );
        
        restartTx();
        
        assertEquals( node, index().getSingleNode( key, value1 ) );
        assertEquals( node,
            this.fulltextIndexService.getSingleNode( key, "Tjena" ) );
        
        index().removeIndex( node, "cpv", value1 );
        this.fulltextIndexService.removeIndex( node, "cpv-ft", value2 );
        node.delete();
    }

    @Test
    public void testKeepLogsConfig()
    {
        Map<String,String> config = new HashMap<String,String>();
        config.put( Config.KEEP_LOGICAL_LOGS, "nioneodb,lucene,lucene-fulltext" );
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( 
                "target/configdb", config );
        IndexService index = new LuceneIndexService( db );
        IndexService fulltext = new LuceneFulltextIndexService( db );
        XaDataSourceManager xaDsMgr = 
            db.getConfig().getTxModule().getXaDataSourceManager();
        assertTrue( xaDsMgr.getXaDataSource( "nioneodb" ).isLogicalLogKept() );
        assertTrue( xaDsMgr.getXaDataSource( "lucene" ).isLogicalLogKept() );
        assertTrue( xaDsMgr.getXaDataSource( 
                "lucene-fulltext" ).isLogicalLogKept() );
        db.shutdown();
        index.shutdown();
        fulltext.shutdown();
        
        config.remove( Config.KEEP_LOGICAL_LOGS ); 
        db = new EmbeddedGraphDatabase( "target/configdb", config );
        index = new LuceneIndexService( db );
        fulltext = new LuceneFulltextIndexService( db );
        xaDsMgr = db.getConfig().getTxModule().getXaDataSourceManager();
        assertTrue( !xaDsMgr.getXaDataSource( "nioneodb" ).isLogicalLogKept() );
        assertTrue( !xaDsMgr.getXaDataSource( "lucene" ).isLogicalLogKept() );
        assertTrue( !xaDsMgr.getXaDataSource( 
                "lucene-fulltext" ).isLogicalLogKept() );
        db.shutdown();
        index.shutdown();
        fulltext.shutdown();

        config.put( Config.KEEP_LOGICAL_LOGS, Boolean.TRUE.toString() ); 
        db = new EmbeddedGraphDatabase( "target/configdb", config );
        index = new LuceneIndexService( db );
        fulltext = new LuceneFulltextIndexService( db );
        xaDsMgr = db.getConfig().getTxModule().getXaDataSourceManager();
        assertTrue( xaDsMgr.getXaDataSource( "nioneodb" ).isLogicalLogKept() );
        assertTrue( xaDsMgr.getXaDataSource( "lucene" ).isLogicalLogKept() );
        assertTrue( xaDsMgr.getXaDataSource( "lucene-fulltext" ).isLogicalLogKept() );
        db.shutdown();
        index.shutdown();
        fulltext.shutdown();
    }
}

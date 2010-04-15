package examples;

import java.util.Random;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.index.timeline.Timeline;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class SiteExamples
{
    @Test
    // START SNIPPET: basicIndexing
    public void basicIndexing()
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "my/path" );
        IndexService index = new LuceneIndexService( graphDb );

        // Create a node with a "name" property and index it in the
        // IndexService.
        Node personNode = graphDb.createNode();
        personNode.setProperty( "name", "Thomas Anderson" );
        index.index( personNode, "name", personNode.getProperty( "name" ) );

        // Get the node with the name "Mattias Persson"
        Node node = index.getSingleNode( "name", "Thomas Anderson" );
        // also see index.getNodes method.
        assert personNode.equals( node );
    }
    // END SNIPPET: basicIndexing
    
    @Test
    // START SNIPPET: basicTimelineUsage
    public void basicTimelineUsage() throws Exception
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "my/path" );
        Node rootNode = graphDb.createNode();
        Timeline timeline = new Timeline( "my_timeline", rootNode, graphDb );

        // Add nodes to your timeline
        long startTime = System.currentTimeMillis();
        for ( int i = 0; i < 50000; i++ )
        {
            timeline.addNode( graphDb.createNode(), System.currentTimeMillis() );
            Thread.sleep( new Random().nextInt( 30 ) );
        }

        // Get all the nodes in the timeline
        timeline.getAllNodes();
        // All nodes after timestamp (3 seconds after the start time)
        timeline.getAllNodesAfter( startTime + 3000 );
        // All nodes before timestamp
        timeline.getAllNodesBefore( System.currentTimeMillis() );
        // All nodes between these timestamps
        timeline.getAllNodesBetween( startTime,
                System.currentTimeMillis() - 5000 );
    }
    // END SNIPPET: basicTimelineUsage
}

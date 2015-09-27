package qa;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestLabels;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class ReadMultipleLogEntryVersionsTest
{
    private final String path = "target/test-data/" + getClass().getName();

    @Test
    public void shouldCreateSomeStuffAndCrashAs222() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(
                TargetDirectory.forTest( getClass() ).makeGraphDbDir().getAbsolutePath() );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode( TestLabels.LABEL_ONE );
            Node node2 = db.createNode();
            node2.setProperty( "key", "some very nice value" );
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            tx.success();
        }

        System.exit( 1 );
    }

    @Test
    public void shouldRecoveryThatDbAs224() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );

        // WHEN
        int count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( GlobalGraphOperations.at( db ).getAllNodes() );
            tx.success();
        }

        // THEN
        assertEquals( 1, count );
    }
}

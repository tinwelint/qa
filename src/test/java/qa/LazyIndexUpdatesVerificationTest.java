package qa;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class LazyIndexUpdatesVerificationTest
{
    private final String directory = "C:\\Users\\Matilas\\Desktop\\69DB862DCE05E3E7390CD75B7AE85681";

    @Test
    public void shouldHmm() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory );
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                node.getProperty( "key" );
            }
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }
}

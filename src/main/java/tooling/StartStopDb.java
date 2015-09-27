package tooling;

import java.io.IOException;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class StartStopDb
{
    public static void main( String[] args ) throws IOException
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( args[0] )
                .newGraphDatabase();

//        System.in.read();
        doSomeTransactions( db );

        db.shutdown();
    }

    private static void doSomeTransactions( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode();
                node.addLabel( DynamicLabel.label( "label-" + i ) );
                node.setProperty( "key-" + i, i );
            }
            tx.success();
        }
    }
}

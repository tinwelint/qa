package tooling;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class ZD4513
{
    public static void main( String[] args )
    {
        File storeDir = new File( "starhub-db" );
        System.out.println( storeDir.getAbsolutePath() );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            Node node;
            try ( Transaction tx = db.beginTx() )
            {
                node = db.createNode();
                node.setProperty( "key1", 101L );
                node.setProperty( "key2", "something" );
                node.setProperty( "key3", new byte[10_000] );
                tx.success();
            }

            for ( int i = 0; i < 1_000; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Map<String,Object> properties = node.getAllProperties();
                    properties.forEach( node::setProperty );
                    tx.success();
                }
                if ( i % 1_000 == 0 )
                {
                    System.out.println( "i = " + i );
                }
            }
        }
        finally
        {
            db.shutdown();
        }
    }
}

package tooling;

import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class StartDbWithBackupEnabled
{
    public static void main( String[] args ) throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );

        Future<?> load = new SillyLoad( db, 10, 1000 );

        System.out.println( "Db started, with some load going into it. ENTER to quit" );
        System.in.read();
        load.cancel( false );
        load.get();
        db.shutdown();
    }
}

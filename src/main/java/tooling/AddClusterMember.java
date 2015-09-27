package tooling;

import java.io.File;
import java.io.IOException;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static tooling.StartClusterMaster.server;

public class AddClusterMember
{
    private static final String root = "C:\\Users\\Matilas\\Desktop\\other";

    public static void main( String[] args ) throws IOException, Exception
    {
        int offset = Integer.parseInt( args[0] );
        String dir = new File( root, "" + offset ).getAbsolutePath();
        FileUtils.deleteRecursively( new File( dir ) );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, "10G size" )
                .setConfig( ClusterSettings.server_id, "" + (1+offset) )
                .setConfig( ClusterSettings.cluster_server, server( 5001+offset ) )
                .setConfig( ClusterSettings.initial_hosts, server( 5001 ) + "," + server( 5002 ) + "," + server( 5003 ) )
                .setConfig( OnlineBackupSettings.online_backup_server, server( 6362+offset ) )
                .newGraphDatabase();

        LastCommittedTxMonitor txMonitor = new LastCommittedTxMonitor(
                db.getDependencyResolver().resolveDependency( TransactionIdStore.class ) );

        System.out.println( "up " + offset + ", ENTER for start catching up" );
        System.in.read();
        System.in.skip( System.in.available() );

        UpdatePuller puller = db.getDependencyResolver().resolveDependency( UpdatePuller.class );
        while ( true )
        {
            puller.pullUpdates();
            if ( System.in.available() > 0 )
            {
                break;
            }
        }

//        Future<Object> load = new SillyLoad( db, 1, 500 );
//        System.out.println( "loading " + offset + ", ENTER for shutdown" );
//        System.in.skip( System.in.available() );

//        System.in.read();
//        load.cancel( false );
//        load.get();
        db.shutdown();
    }
}

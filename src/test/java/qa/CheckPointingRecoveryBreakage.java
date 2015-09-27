package qa;

import org.junit.Test;

import java.io.File;

import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;

import static org.neo4j.test.ProcessUtil.executeSubProcess;

public class CheckPointingRecoveryBreakage
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );

    @Test
    public void shouldBreakRecovery() throws Exception
    {
        String dir = "target/checkpoint-breakage";
        for ( int i = 0; i < 100; i++ )
        {
            FileUtils.deleteRecursively( new File( dir ) );
            executeSubProcess( getClass(), 10, MINUTES, dir );

            new GraphDatabaseFactory().newEmbeddedDatabase( new File( dir ) ).shutdown();
            ConsistencyCheckTool.main( new String[] {dir} );
        }
    }

    public static void main( String[] args ) throws Exception
    {
        long checkPointIntervalMs = 500;
        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( new File( args[0] ) )
                .setConfig( GraphDatabaseSettings.check_point_interval_time, String.valueOf( checkPointIntervalMs ) + "ms" )
                .newGraphDatabase();

        for ( int i = 0; i < 100; i++ )
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    try
                    {
                        while ( true )
                        {
                            try ( Transaction tx = db.beginTx() )
                            {
                                Node node = db.createNode();
                                node.createRelationshipTo( node, TYPE );
                                tx.success();
                            }
                        }
                    }
                    catch ( Throwable e )
                    {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

        final long end = currentTimeMillis() + checkPointIntervalMs * 5;
        System.out.println( "end " + end + ", current " + currentTimeMillis() );
        while ( currentTimeMillis() < end )
        {
            Thread.sleep( 200 );
            System.out.println( "end " + end + ", current " + currentTimeMillis() );
        }
        System.out.println( "exit" );
        System.exit( 0 );
    }
}

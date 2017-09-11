package qa;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

import static java.util.concurrent.TimeUnit.MINUTES;

public class CreateBigIndexTest
{
    private static final File DIRECTORY = new File( "big-index-db" );
    private static final Label LABEL = Label.label( "Label" );
    private static final String KEY = "key";

    @Test
    public void shouldCreateBigIndex() throws Exception
    {
        FileUtils.deleteRecursively( DIRECTORY );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DIRECTORY );
        fillData( db );
        System.out.println( "Creating index" );
        createIndex( db );
    }

    private void createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, MINUTES );
            tx.success();
        }
    }

    private void fillData( GraphDatabaseService db ) throws InterruptedException
    {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool( availableProcessors );
        for ( int i = 0; i < availableProcessors; i++ )
        {
            int finalI = i;
            executor.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    for ( int t = 0; t < 100; t++ )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            for ( int j = 0; j < 1_000; j++ )
                            {
                                db.createNode( LABEL ).setProperty( KEY, finalI + "-" + t + "-" + j );
                            }
                            tx.success();
                        }
                        if ( t % 10 == 0 )
                        {
                            System.out.println( finalI + "-" + t );
                        }
                    }
                    return null;
                }
            } );
        }
        executor.shutdown();
        executor.awaitTermination( 10, MINUTES );
    }
}

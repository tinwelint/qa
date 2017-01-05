package qa;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.ThreadTestUtils;

import static org.junit.Assert.assertEquals;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SimplifiedNeoStoreDataSourceStopTest
{
    @Test
    public void shouldWaitForCommittingInFlightTransactionsAWhile() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        int txs = 10;
        ExecutorService executor = Executors.newFixedThreadPool( txs + 1 );

        // WHEN
        AtomicBoolean end = new AtomicBoolean();
        for ( int i = 0; i < txs; i++ )
        {
            executor.submit( tx( db, i ) );
        }
        executor.submit( () ->
        {
            while ( !end.get() )
            {
                try
                {
                    Thread.sleep( 1_000 );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                ThreadTestUtils.dumpAllStackTraces();
            }
        } );

        // THEN
        db.shutdown();
        end.set( true );
        List<Runnable> left = executor.shutdownNow();
        assertEquals( 0, left.size() );
    }

    private Runnable tx( GraphDatabaseService db, int i )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                try
                {
                    Thread.sleep( SECONDS.toMillis( i ) );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                tx.success();
            }
        };
    }
}

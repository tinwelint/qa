package tooling;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class SillyLoad implements Future<Object>
{
    private final AtomicBoolean cancel = new AtomicBoolean();
    private final AtomicInteger done = new AtomicInteger();
    private final int threads;
    private final Label label = DynamicLabel.label( "PERFORMANCE_BITCH" );

    public SillyLoad( final GraphDatabaseService db, int threads, final long delayInBetweenTxs ) throws Exception
    {
        this.threads = threads;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                db.schema().indexFor( label ).on( "key-" + i ).create();
            }
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }

        for ( int i = 0; i < threads; i++ )
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    try
                    {
                        while ( !cancel.get() )
                        {
                            try
                            {
                                addSillyData();
                                if ( delayInBetweenTxs > 0 )
                                {
                                    sleepAWhile( delayInBetweenTxs );
                                }
                            }
                            catch ( Throwable e )
                            {
                                e.printStackTrace();
                                sleepAWhile( 1000 );
                            }
                        }
                    }
                    finally
                    {
                        done.incrementAndGet();
                    }
                }

                private void sleepAWhile( final long delayInBetweenTxs )
                {
                    try
                    {
                        Thread.sleep( delayInBetweenTxs );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                }

                private void addSillyData()
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( int j = 0; j < 100; j++ )
                        {
                            Node node = db.createNode( label );
                            node.setProperty( "key-" + j, UUID.randomUUID().toString() );
                        }
                        tx.success();
                    }
                }
            }.start();
        }
    }

    @Override
    public boolean cancel( boolean mayInterruptIfRunning )
    {
        cancel.set( true );
        return true;
    }

    @Override
    public boolean isCancelled()
    {
        return cancel.get();
    }

    @Override
    public boolean isDone()
    {
        return threads != done.get();
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException
    {
        while ( !isDone() )
        {
            Thread.sleep( 100 );
        }
        return null;
    }

    @Override
    public Object get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException,
            TimeoutException
    {
        return get();
    }
}

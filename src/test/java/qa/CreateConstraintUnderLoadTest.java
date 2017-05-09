package qa;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.Long.min;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CreateConstraintUnderLoadTest
{
    private static final String KEY = "key";
    private static final Label LABEL = Label.label( "Label4" );

    private GraphDatabaseService db;
    private int highNodeId;

    @Before
    public void startDb()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( "E:\\graph.db30" ) );
        highNodeId = (int) ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores().getNodeStore().getHighId();
    }

    @After
    public void stopDb()
    {
        db.shutdown();
    }

    @Test
    public void shouldClearProperties() throws Exception
    {
        // GIVEN
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool( threads );
        long highNodeId = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores().getNodeStore().getHighId();

        long perThread = highNodeId / threads;
        long fromNodeId = 0;
        long toNodeId = perThread;
        System.out.println( "perThread " + perThread );
        for ( int i = 0; i < threads; i++ )
        {
            long finalFromNodeId = fromNodeId;
            long finalToNodeId = toNodeId;
            executor.submit( () ->
            {
                Transaction tx = db.beginTx();
                int count = 0;
                for ( long nodeId = finalFromNodeId; nodeId < finalToNodeId; nodeId++ )
                {
                    db.getNodeById( nodeId ).removeProperty( KEY );
                    if ( ++count % 10_000 == 0 )
                    {
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                        if ( count % 1_000_000 == 0 )
                        {
                            System.out.println( Thread.currentThread().getName() + " at " + count );
                        }
                    }
                }
                tx.success();
                tx.close();
            } );

            fromNodeId = toNodeId;
            toNodeId += min( highNodeId, perThread );
        }
        executor.shutdown();
        executor.awaitTermination( 10, MINUTES );
    }

    @Test
    public void shouldCreateConstraintUnderLoad() throws Exception
    {
        clearSchema();

        int threads = 5;
        ExecutorService executor = Executors.newFixedThreadPool( threads + 1 );
        AtomicBoolean end = new AtomicBoolean();
        AtomicLong txs = new AtomicLong();
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( () ->
            {
                try
                {
                    while ( !end.get() )
                    {
                        try
                        {
                            transactionHoggingSchemaReadLock( txs );
                        }
                        catch ( TransientFailureException e )
                        {
                            System.out.println( "Transient..." );
                        }
                    }
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            } );
        }
        executor.submit( () ->
        {
            long last = 0;
            while ( !end.get() )
            {
                Thread.sleep( 2_000 );
                long current = txs.get();
                long delta = current - last;
                last = current;
                System.out.println( "At " + current + " Δ" + delta );
            }
            return null;
        } );
        System.out.println( "Load started... waiting a bit" );

        // WHEN
        Thread.sleep( 10_000 );
        try ( Transaction tx = db.beginTx() )
        {
            System.out.println( "Trying to grab schema write lock" );
            // Label4 should be around 500/2/2/2 = 75M nodes
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( KEY ).create();
//            db.schema().indexFor( LABEL ).on( KEY ).create();
            System.out.println( "Got schema write lock" );
            tx.success();
        }
        System.out.println( "Done, waiting a bit more" );
        Thread.sleep( 30_000 );
        System.out.println( "Shutting down" );

        end.set( true );
        executor.shutdown();
        executor.awaitTermination( 1, TimeUnit.MINUTES );
    }

    private void clearSchema()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                constraint.drop();
                System.out.println( "Dropped " + constraint );
            }
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                index.drop();
                System.out.println( "Dropped " + index );
            }
            tx.success();
        }
    }

    private void transactionHoggingSchemaReadLock( AtomicLong txs )
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 5; i++ )
            {
                db.getNodeById( random.nextInt( highNodeId ) ).setProperty( KEY, UUID.randomUUID().toString() );
            }
            tx.success();
            txs.incrementAndGet();
        }
    }
}

package qa;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.RandomRule;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

public class ZD3614
{
    private static final String KEY = "key";
    private static final String KEY2 = "key2";
    private static final String KEY1 = "key1";
    private static final int TX_SIZE = 15_000;
    private static final String PATH = "K:\\db";

    private GraphDatabaseService db;
    private Index<Relationship> relIndex1;
    private Index<Relationship> relIndex2;
    private Index<Node> nodeIndex;

    @Rule
    public final RepeatRule repeat = new RepeatRule();
    @Rule
    public final RandomRule random = new RandomRule();

    public void start()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabase( PATH );
        try ( Transaction tx = db.beginTx() )
        {
            relIndex1 = db.index().forRelationships( "yoyo" );
            relIndex2 = db.index().forRelationships( "yoyo2" );
            nodeIndex = db.index().forNodes( "sdjk" );
            tx.success();
        }
    }

    @After
    public void shutdown()
    {
        db.shutdown();
    }

    @Repeat( times = 1_000 )
    @Test
    public void shouldTest() throws Throwable
    {
        FileUtils.deleteRecursively( new File( PATH ) );
        start();
        final AtomicBoolean halt = new AtomicBoolean();
        final AtomicInteger idStarted = new AtomicInteger();
        final AtomicInteger idCommitted = new AtomicInteger();
        final AtomicReference<Throwable> error = new AtomicReference<>();

        // Initial transaction creating base data
        Node[] nodes = new Node[TX_SIZE];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < TX_SIZE; i++ )
            {
                nodes[i] = db.createNode();
                nodes[i].setProperty( KEY, i );
                nodeIndex.add( nodes[i], KEY, i );
            }
            for ( int i = 0; i < TX_SIZE; i++ )
            {
                nodes[i].createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            }
            db.createNode().setProperty( KEY1, true );
            db.createNode().setProperty( KEY2, true );
            tx.success();
        }

        long txIdBefore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        Thread input = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    while ( !halt.get() )
                    {
                        // Perform big transaction
                        int id = idStarted.incrementAndGet();
                        try ( Transaction tx = db.beginTx() )
                        {
                            // Create relationships, this takes care of:
                            // - AddRelationship
                            // - +Relationships
                            // - +Properties
                            for ( int i = 0; i < TX_SIZE; i++ )
                            {
                                Relationship newRel = nodes[i].createRelationshipTo( db.createNode(), MyRelTypes.TEST );
                                String value1 = "" + id;
                                newRel.setProperty( KEY1, value1 );
                                relIndex1.add( newRel, KEY1, value1 );

                                String value2 = "" + id;
                                newRel.setProperty( KEY2, value2 );
                                relIndex2.add( newRel, KEY2, value2 );
                            }

                            // Change node properties, this takes care of:
                            // - Update node properties
                            // - Add/remove node index
                            for ( int i = 0; i < TX_SIZE; i++ )
                            {
                                Node node = nodes[i];
                                Object prop = node.removeProperty( KEY );
                                nodeIndex.remove( node, KEY, prop );

                                node.setProperty( KEY, prop );
                                nodeIndex.add( node, KEY, prop );
                            }

                            tx.success();
                        }
                        idCommitted.incrementAndGet();
                    }
                }
                catch ( Throwable e )
                {
                    // This is OK (pretend)
                    System.out.println( e );
                }
            }
        };
        input.start();

        // Let a couple of transactions come in
        int time =
//                10_000;
                random.intBetween( 1_000, 10_000 );
        Thread.sleep( time );

        db.shutdown();
        halt.set( true );
        while ( input.isAlive() )
        {
            try
            {
                Thread.sleep( 10 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
        start();

        if ( error.get() != null )
        {
            throw error.get();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < TX_SIZE; i++ )
            {
                nodes[i] = db.getNodeById( nodes[i].getId() );
            }
            tx.success();
        }

        long txIdAfter = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        System.out.println( "started:" + idStarted + ", committed:" + idCommitted + " seems to have committed " +
                (txIdAfter - txIdBefore) + " transactions" );

        int id = idCommitted.get();
        System.out.println( "Verifying " + id );

        assertEquals( (txIdAfter - txIdBefore), id );
        try ( Transaction tx = db.beginTx() )
        {
            // Verify id
            for ( int i = 0; i < TX_SIZE; i++ )
            {
                Node node = nodes[i];
                assertEquals( node, nodeIndex.get( KEY, node.getProperty( KEY ) ).getSingle() );
            }
            assertEquals( TX_SIZE, count( relIndex1.get( KEY1, "" + id ) ) );
            assertEquals( TX_SIZE, count( relIndex2.get( KEY2, "" + id ) ) );
            tx.success();
        }
    }
}

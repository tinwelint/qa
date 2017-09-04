package perf;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.Randoms;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;

import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.io.ByteUnit.mebiBytes;

public class RecoveryPerformanceTest
{
    // Teh control panelz
    private static final int INITIAL_DATA_TRANSACTIONS = 1_000;
    private static final int INITIAL_DATA_TRANSACTION_SIZE = 100;
    private static final int ONLINE_TRANSACTIONS = 10_000;
    private static final int ONLINE_TRANSACTION_SIZE = 100;
    private static final int INDEXES = 5;

    private static final File DIRECTORY = new File( "recovery-db" );
    private static final String[] KEYS = new String[] {"key1", "key2", "key3", "key4", "key5"};
    private enum Labels implements Label
    {
        LABEL1,
        LABEL2,
        LABEL3,
        LABEL4,
        LABEL5,
    }
    private static final Label[] LABELS = Labels.values();
    private static final RelationshipType[] TYPES = MyRelTypes.values();

    public static void main( String[] args ) throws Exception
    {
        Monitors monitors = new Monitors();
        AtomicInteger count = new AtomicInteger();
        AtomicLong durationOfReverseRecovery = new AtomicLong();
        AtomicLong durationOfForwardRecovery = new AtomicLong();
        monitors.addMonitorListener( new Recovery.Monitor()
        {
            long startTime;

            @Override
            public void recoveryRequired( LogPosition recoveryPosition )
            {
                System.out.println( "Starting recovery at " + recoveryPosition );
                startTime = currentTimeMillis();
            }

            @Override
            public void reverseStoreRecoveryCompleted( long checkpointTxId )
            {
                System.out.println( "Reverse recovery completed" );
                durationOfReverseRecovery.set( currentTimeMillis() - startTime );
                startTime = currentTimeMillis();
            }

            @Override
            public void recoveryCompleted( int numberOfRecoveredTransactions )
            {
                System.out.println( "Forward recovery completed" );
                count.set( numberOfRecoveredTransactions );
                durationOfForwardRecovery.set( currentTimeMillis() - startTime );
            }
        } );
        GraphDatabaseService db = new GraphDatabaseFactory().setMonitors( monitors ).newEmbeddedDatabaseBuilder( DIRECTORY )
                // I.e. control checkpointing manually
                .setConfig( GraphDatabaseSettings.check_point_interval_time, "1h" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "1000000000" )
                .newGraphDatabase();

        if ( count.get() > 0 )
        {
            System.out.println( "Recovered " + count + " transactions in" +
                    " reverse " + duration( durationOfReverseRecovery.get() ) +
                    " forward " + duration( durationOfForwardRecovery.get() ) );
            db.shutdown();
            return;
        }

        // Build initial data
        System.out.println( "Creating schema" );
        createSchema( db );

        int threads = Runtime.getRuntime().availableProcessors();
        System.out.println( "Creating initial data" );
        createInitialData( db, threads );

        // Checkpoint
        System.out.println( "Checkpointing" );
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "Manual" ) );

        // Run random operations, lots of them
        System.out.println( "Doing lots of operations" );
        doLotsOfRandomOperations( db, threads );

        // Crash
        System.exit( 0 );
    }

    private static void createSchema( GraphDatabaseService db )
    {
        Randoms random = newRandoms();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < INDEXES; i++ )
            {
                IndexCreator creator = db.schema().indexFor( random.among( LABELS ) );
                for ( String key : random.selection( KEYS, 1, 2, false ) )
                {
                    creator = creator.on( key );
                }
                try
                {
                    creator.create();
                }
                catch ( ConstraintViolationException e )
                {   // It's OK
                    i--;
                }
            }
            tx.success();
        }
    }

    @Test
    public void shouldReadThroughLogStream() throws Exception
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        try ( PageCache pageCache = new ConfiguringPageCacheFactory( fs, Config.defaults(), PageCacheTracer.NULL,
                PageCursorTracerSupplier.NULL, NullLog.getInstance() ).getOrCreatePageCache() )
        {
            StoreFactory storeFactory = new StoreFactory( DIRECTORY, pageCache, fs, NullLogProvider.getInstance() );
            try ( NeoStores neoStores = storeFactory.openNeoStores( StoreType.META_DATA ) )
            {
                LogVersionRepository logVersionRepository = neoStores.getMetaDataStore();
                LifeSupport life = new LifeSupport();
                try
                {
                    LogFile logFile = life.add( new PhysicalLogFile( fs, new PhysicalLogFiles( DIRECTORY, fs ), mebiBytes( 250 ),
                            () -> 10L, logVersionRepository, PhysicalLogFile.NO_MONITOR, new LogHeaderCache( 10 ) ) );
                    LogicalTransactionStore store = new PhysicalLogicalTransactionStore( logFile, new TransactionMetadataCache( 10 ),
                            new VersionAwareLogEntryReader<>() );
                    life.start();

                    long time = currentTimeMillis();
                    int count = 0;
                    try ( TransactionCursor txCursor = store.getTransactionsInReverseOrder( LogPosition.start( 0 ) ) )
                    {
                        while ( txCursor.next() )
                        {
                            count++;
                        }
                    }
                    long duration = currentTimeMillis() - time;
                    System.out.println( count + " in " + duration( duration ) );
                }
                finally
                {
                    life.shutdown();
                }
            }
        }
    }

    private static void doLotsOfRandomOperations( GraphDatabaseService db, int threads ) throws Exception
    {
        ExecutorService executor = newFixedThreadPool( threads );
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( () ->
            {
                Randoms random = newRandoms();
                for ( int j = 0; j < ONLINE_TRANSACTIONS; j++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( int k = 0; k < ONLINE_TRANSACTION_SIZE; )
                        {
                            try
                            {
                                doRandomOperation( db, random );
                                k++;
                            }
                            catch ( DeadlockDetectedException e )
                            {
                                throw e;
                            }
                            catch ( Exception e )
                            {
                                // It's fine
                            }
                        }
                        tx.success();
                    }
                    catch ( Exception e )
                    {
                        // it's fine
                        j--;
                    }

                    if ( j % 100 == 0 && j > 0 )
                    {
                        System.out.println( Thread.currentThread().getName() + " " + j );
                    }
                }
            } );
        }
        executor.shutdown();
        executor.awaitTermination( 10, TimeUnit.MINUTES );
    }

    private static Randoms newRandoms()
    {
        return new Randoms( ThreadLocalRandom.current(), new Randoms.Configuration()
        {
            @Override
            public int stringMinLength()
            {
                return 5;
            }

            @Override
            public int stringMaxLength()
            {
                return 120;
            }

            @Override
            public int stringCharacterSets()
            {
                return Randoms.CSA_LETTERS_AND_DIGITS;
            }

            @Override
            public int arrayMinLength()
            {
                return 2;
            }

            @Override
            public int arrayMaxLength()
            {
                return 30;
            }
        } );
    }

    private static void doRandomOperation( GraphDatabaseService db, Randoms random )
    {
        float operationType = random.nextFloat();
        float operation = random.nextFloat();
        if ( operationType < 0.5 )
        {   // create
            if ( operation < 0.5 )
            {   // create node (w/ random label, prop)
                Node node = db.createNode( random.nextBoolean() ? array( random.among( LABELS ) ) : new Label[0] );
                if ( random.nextBoolean() )
                {
                    node.setProperty( random.among( KEYS ), random.propertyValue() );
                }
            }
            else
            {   // create relationship (w/ random prop)
                Relationship relationship = randomNode( db, random )
                        .createRelationshipTo( randomNode( db, random ), random.among( TYPES ) );
                setRandomProperties( relationship, random );
            }
        }
        else if ( operationType < 0.8 )
        {   // change
            if ( operation < 0.25 )
            {   // add label
                randomNode( db, random ).addLabel( random.among( LABELS ) );
            }
            else if ( operation < 0.5 )
            {   // remove label
                randomNode( db, random ).removeLabel( random.among( LABELS ) );
            }
            else if ( operation < 0.75 )
            {   // set node property
                setRandomProperties( randomNode( db, random ), random );
            }
            else
            {   // set relationship property
                setRandomProperties( randomRelationship( db, random ), random );
            }
        }
        else
        {   // delete
            if ( operation < 0.25 )
            {   // remove node property
                randomNode( db, random ).removeProperty( random.among( KEYS ) );
            }
            else if ( operation < 0.5 )
            {   // remove relationship property
                randomRelationship( db, random ).removeProperty( random.among( KEYS ) );
            }
            else if ( operation < 0.9 )
            {   // delete relationship
                randomRelationship( db, random ).delete();
            }
            else
            {   // delete node
                Node node = randomNode( db, random );
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationship.delete();
                }
                node.delete();
            }
        }
    }

    private static Relationship randomRelationship( GraphDatabaseService db, Randoms random )
    {
        long highId = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getRelationshipStore().getHighId();
        while ( true )
        {
            try
            {
                return db.getRelationshipById( random.nextLong( highId ) );
            }
            catch ( NotFoundException e )
            {   // it's OK
            }
        }
    }

    private static Node randomNode( GraphDatabaseService db, Randoms random )
    {
        long highId = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getNodeStore().getHighId();
        while ( true )
        {
            try
            {
                return db.getNodeById( random.nextLong( highId ) );
            }
            catch ( NotFoundException e )
            {   // it's OK
            }
        }
    }

    private static void createInitialData( GraphDatabaseService db, int threads ) throws InterruptedException
    {
        ExecutorService executor = newFixedThreadPool( threads );
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( () ->
            {
                Randoms random = newRandoms();
                for ( int j = 0; j < INITIAL_DATA_TRANSACTIONS; j++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( int k = 0; k < INITIAL_DATA_TRANSACTION_SIZE; k++ )
                        {
                            Node node1 = db.createNode( random.selection( LABELS, 0, 2, false ) );
                            Node node2 = db.createNode( random.selection( LABELS, 0, 2, false ) );
                            setRandomProperties( node1, random );
                            setRandomProperties( node2, random );
                            int relationships = random.intBetween( 1, 20 );
                            for ( int l = 0; l < relationships; l++ )
                            {
                                Node startNode = random.nextBoolean() ? node1 : node2;
                                Node endNode = random.nextBoolean() ? node1 : node2;
                                startNode.createRelationshipTo( endNode, random.among( TYPES ) );
                            }
                        }
                        tx.success();
                    }

                    if ( j % 100 == 0 && j > 0 )
                    {
                        System.out.println( Thread.currentThread().getName() + " " + j );
                    }
                }
                return null;
            } );
        }
        executor.shutdown();
        executor.awaitTermination( 10, TimeUnit.MINUTES );
    }

    private static void setRandomProperties( PropertyContainer entity, Randoms random )
    {
        for ( String key : random.selection( KEYS, 0, KEYS.length, false ) )
        {
            entity.setProperty( key, random.propertyValue() );
        }
    }
}

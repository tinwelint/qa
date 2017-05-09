package qa;

import org.junit.Test;

import java.io.File;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TestStoreCopyAndCheckPoint
{
    // === CONTROL CENTER ===
    private static final boolean KEEP_OLD_SNAPSHOT_OPEN_WHEN_HANDLING_STORE_COPY_REQUEST = true;
    // ======================

    private static final Label LABEL = Label.label( "Label" );
    private static final String KEY = "key";

    @Test
    public void shouldTest() throws Exception
    {
        File storeDir = new File( "yo" ).getAbsoluteFile();
        FileUtils.deleteRecursively( storeDir );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        createIndex( db );

        // Some data
        createNodes( db, 1, 100 );

        // Some backup request which hangs or something
        ResourceIterator<File> oldHoggedSnapshot =
                db.getDependencyResolver().resolveDependency( IndexingService.class ).snapshotStoreFiles();

        // System continues
        createNodes( db, 100, 10_000 );

        if ( !KEEP_OLD_SNAPSHOT_OPEN_WHEN_HANDLING_STORE_COPY_REQUEST )
        {
            oldHoggedSnapshot.close();
        }

        // Our store copy request first does check point
        db.getDependencyResolver().resolveDependency( CheckPointer.class )
                .forceCheckPoint( new SimpleTriggerInfo( "BAJS" ) );

        // And then copies files
        try ( ResourceIterator<File> ourSnapshot =
                db.getDependencyResolver().resolveDependency( IndexingService.class ).snapshotStoreFiles() )
        {
//            File target = new File( "target" ).getAbsoluteFile();
//            FileUtils.deleteRecursively( target );
            while ( ourSnapshot.hasNext() )
            {
                System.out.println( ourSnapshot.next() );
//                File sourceFile = ourSnapshot.next();
//                Path rel = sourceFile.toPath().relativize( storeDir.toPath() );
//                System.out.println( rel );
//                Path targetPath = rel.resolve( target.toPath() );
//                System.out.println( sourceFile + " --> " + targetPath.toAbsolutePath() );
            }
        }

        if ( KEEP_OLD_SNAPSHOT_OPEN_WHEN_HANDLING_STORE_COPY_REQUEST )
        {
            oldHoggedSnapshot.close();
        }

        db.shutdown();
    }

    private void createNodes( GraphDatabaseAPI db, int txCount, int countPerTx )
    {
        for ( int t = 0; t < txCount; t++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int n = 0; n < countPerTx; n++ )
                {
                    db.createNode( LABEL ).setProperty( KEY, true );
                }
                tx.success();
            }
        }
    }

    private void createIndex( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }
}

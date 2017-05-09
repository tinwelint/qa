package tooling;

import com.hazelcast.internal.util.ThreadLocalRandom;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.index.internal.gbptree.GenSafePointer;
import org.neo4j.index.internal.gbptree.IdSpace;
import org.neo4j.index.internal.gbptree.TreeNode;
import org.neo4j.index.internal.gbptree.TreeState;
import org.neo4j.index.internal.gbptree.TreeStatePair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanStore.Monitor;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.labelscan.LabelScanKey;
import org.neo4j.kernel.impl.index.labelscan.LabelScanLayout;
import org.neo4j.kernel.impl.index.labelscan.LabelScanValue;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;

import static java.lang.Long.min;
import static java.lang.Math.toIntExact;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

public class LabelScanStoreLoad
{
    public static void main( String[] argss ) throws IOException
    {
        Args args = Args.parse( argss );
        File storeDir = new File( args.get( "into" ) );
        Action action = Action.valueOf( args.get( "action" ) );

        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                PageCache pageCache = new ConfiguringPageCacheFactory( fs, Config.empty(),
                        PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, NullLog.getInstance() )
                        .getOrCreatePageCache() )
        {
            if ( action == Action.recover || action == Action.insert )
            {
                LifeSupport life = new LifeSupport();
                LabelScanStore store = life.add( new NativeLabelScanStore( pageCache, storeDir, FullStoreChangeStream.EMPTY,
                        false, printingMonitor() ) );
                life.init();
                try
                {
                    if ( action == Action.recover )
                    {
                        System.out.println( "Provoking recovery" );
                        store.newWriter().close();
                    }
                    life.start();

                    if ( action == Action.insert )
                    {
                        long nodes = Long.parseLong( ununderscorify( args.get( "nodes" ) ) );
                        int numberOfLabels = args.getNumber( "labels", 1 ).intValue();
                        try ( LabelScanWriter writer = store.newWriter() )
                        {
                            String process = "Inserting " + nodes + " nodes with " +
                                    numberOfLabels + " labels each";
                            ProgressListener progress =
                                    ProgressMonitorFactory.textual( System.out ).singlePart( process, nodes );
                            long[] labels = createLabelsArray( numberOfLabels );
                            for ( long nodeId = 0; nodeId < nodes; nodeId++ )
                            {
                                writer.write( labelChanges( nodeId, EMPTY_LONG_ARRAY, labels ) );
                                if ( nodeId % 10_000 == 0 )
                                {
                                    progress.set( nodeId );
                                }
                            }
                            progress.set( nodes );
                        }
                    }
                }
                finally
                {
                    life.shutdown();
                }
            }
            else if ( action == Action.crash )
            {
                long percent = Long.parseLong( ununderscorify( args.get( "percent" ) ) );
                try ( PagedFile pagedFile = pageCache.map( new File( storeDir, NativeLabelScanStore.FILE_NAME ),
                        pageCache.pageSize(), StandardOpenOption.WRITE );
                        PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
                {
                    long crashGeneration = readCrashGeneration( pagedFile );
                    long highestPageId = pagedFile.getLastPageId();
                    long pointersToCrash = (long)(highestPageId * (percent / 100D));
                    int meanStride = toIntExact( highestPageId / pointersToCrash );
                    long pageId = IdSpace.MIN_TREE_NODE_ID;
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    LabelScanLayout layout = new LabelScanLayout();
                    TreeNode<LabelScanKey,LabelScanValue> treeNode = new TreeNode<>( pageCache.pageSize(), layout );
                    ProgressListener progress = ProgressMonitorFactory.textual( System.out )
                            .singlePart( "Crashing " + pointersToCrash + " pointers, which is roughly " +
                                    percent + "% of " + highestPageId + " pages", pointersToCrash );
                    int crashCount = 0;
                    while ( pageId < highestPageId )
                    {
                        pageId = min( highestPageId, pageId + random.nextInt( meanStride * 2 ) );
                        if ( !cursor.next( pageId ) )
                        {
                            throw new IllegalStateException( "Couldn't go to page " + pageId );
                        }
                        if ( crashRandomPointer( cursor, random, treeNode, crashGeneration ) )
                        {
                            crashCount++;
                        }
                        progress.add( 1 );
                    }
                    progress.set( pointersToCrash );
                    System.out.println( "Actually crashed pointers: " + crashCount );
                }
            }
        }
    }

    private static Monitor printingMonitor()
    {
        return new Monitor()
        {
            @Override
            public void recoveryCompleted( Map<String,Object> data )
            {
                System.out.println( data );
            }
        };
    }

    private static long readCrashGeneration( PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            Pair<TreeState,TreeState> states = TreeStatePair.readStatePages( cursor, 1, 2 );
            TreeState state = TreeStatePair.selectNewestValidState( states );
            return state.unstableGeneration();
        }
    }

    private static boolean crashRandomPointer( PageCursor cursor, ThreadLocalRandom random,
            TreeNode<LabelScanKey,LabelScanValue> treeNode, long crashGeneration )
    {
        if ( TreeNode.nodeType( cursor ) != TreeNode.NODE_TYPE_TREE_NODE )
        {
            return false;
        }

        int max = TreeNode.isInternal( cursor ) ? 4 : 3;
        switch ( random.nextInt( max ) )
        {
        case 0: crashPointerByOffset( cursor, TreeNode.BYTE_POS_NEWGEN, random, crashGeneration ); break;
        case 1: crashPointerByOffset( cursor, TreeNode.BYTE_POS_LEFTSIBLING, random, crashGeneration ); break;
        case 2: crashPointerByOffset( cursor, TreeNode.BYTE_POS_RIGHTSIBLING, random, crashGeneration ); break;
        case 3: // child
            int child = random.nextInt( treeNode.keyCount( cursor ) );
            crashPointerByOffset( cursor, treeNode.childOffset( child ), random, crashGeneration );
            break;
        }
        return true;
    }

    private static void crashPointerByOffset( PageCursor cursor, int offset, ThreadLocalRandom random,
            long crashGeneration )
    {
        boolean slotA = random.nextBoolean();
        cursor.setOffset( offset + (slotA ? 0 : GenSafePointer.SIZE) );
        GenSafePointer.readGeneration( cursor );
        long pointer = GenSafePointer.readPointer( cursor );
        cursor.setOffset( offset + (slotA ? 0 : GenSafePointer.SIZE) );
        GenSafePointer.write( cursor, crashGeneration, pointer );
    }

    private static String ununderscorify( String text )
    {
        return text.replaceAll( "_", "" );
    }

    private static long[] createLabelsArray( int numberOfLabels )
    {
        long[] labels = new long[numberOfLabels];
        for ( int i = 0; i < numberOfLabels; i++ )
        {
            labels[i] = i;
        }
        return labels;
    }

    private enum Action
    {
        insert,
        recover,
        crash
    }
}

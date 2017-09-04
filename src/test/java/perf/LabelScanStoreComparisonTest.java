/**
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package perf;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.textual;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.test.rule.PageCacheRule.config;

@RunWith( Parameterized.class )
public class LabelScanStoreComparisonTest
{
    @Parameters( name = "{2}_{3}" )
    public static Collection<Object[]> runs()
    {
        Collection<Object[]> runs = new ArrayList<>();
        runs.add( array( 100_000, 100, Stores.NATIVE, Loads.RANDOM ) );
        runs.add( array( 100_000_000_000L, 10_000, Stores.NATIVE, Loads.SEQUENTIAL ) );
        return runs;
    }

    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule( config().withInconsistentReads( false ) );
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass() ).keepDirectoryAfterSuccessfulTest();
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final LifeRule life = new LifeRule( false );
    @Rule
    public final TestName testName = new TestName();

    @Parameter( 0 )
    public int count;
    @Parameter( 1 )
    public int txSize = 100;
    @Parameter( 2 )
    public BiFunction<Supplier<PageCache>,File,LabelScanStore> factory;
    @Parameter( 3 )
    public Load load;

    interface Load
    {
        void apply( LabelScanStore store, ProgressListener progress, int count, int txSize, Random random )
                throws IOException;
    }

    @Test
    public void shouldPutLabelScanStoreUnderLoad() throws Exception
    {
        // GIVEN
        LabelScanStore labelScanStore = life.add( factory.apply(
                () -> pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() ), null ) );

        // WHEN/THEN
        long time = currentTimeMillis();
        ProgressListener progress = textual( System.out ).singlePart( "Insert:" + testName.getMethodName(), count );
        load.apply( labelScanStore, progress, count, txSize, random.random() );
        long writeTime = currentTimeMillis() - time;
        System.out.println( "write:" + duration( writeTime ) );

//        // === READ ===
//        time = currentTimeMillis();
//        try ( final LabelScanReader reader = labelScanStore.newReader() )
//        {
//            for ( int labelId = 1; labelId <= 2; labelId++ )
//            {
//                final PrimitiveLongIterator nodeIds = reader.nodesWithLabel( labelId );
//                PrimitiveLongCollections.count( nodeIds ); // to loop through it
//            }
//        }
//        long readTime = currentTimeMillis() - time;
//        System.out.println( "read:" + duration( readTime ) );
    }

    @Ignore
    @Test
    public void shouldInvokeCrashCleaner() throws Exception
    {
        // GIVEN
        LabelScanStore labelScanStore = life.add( factory.apply(
                () -> pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() ),
                testDirectory.absolutePath() ) );
        life.init();
        labelScanStore.newWriter().close();
        life.start();

        life.shutdown();
    }

    enum Stores implements BiFunction<Supplier<PageCache>,File,LabelScanStore>
    {
        NATIVE
        {
            @Override
            public LabelScanStore apply( Supplier<PageCache> pageCache, File storeDir )
            {
                return new NativeLabelScanStore( pageCache.get(), storeDir, FullStoreChangeStream.EMPTY, false,
                        new Monitors(), RecoveryCleanupWorkCollector.IMMEDIATE );
            }
        };
    }

    enum Loads implements Load
    {
        RANDOM
        {
            @Override
            public void apply( LabelScanStore store, ProgressListener progress, int count, int txSize, Random random )
                    throws IOException
            {
                int txs = count / txSize;
                long[] ids = new long[txSize];
                for ( int t = 0; t < txs; t++ )
                {
                    for ( int i = 0; i < txSize; i++ )
                    {
                        ids[i] = random.nextInt( 100_000_000 );
                    }
                    Arrays.sort( ids );
                    try ( LabelScanWriter writer = store.newWriter() )
                    {
                        for ( int i = 0; i < ids.length; i++ )
                        {
                            writer.write( labelChanges( ids[i], EMPTY_LONG_ARRAY, someLabels() ) );
                        }
                    }
                    progress.add( txSize );
                }
            }
        },
        SEQUENTIAL
        {
            @Override
            public void apply( LabelScanStore store, ProgressListener progress, int count, int txSize, Random random )
                    throws IOException
            {
                try ( final LabelScanWriter writer = store.newWriter() )
                {
                    int batchSize = 1_000;
                    int batches = count / batchSize;
                    for ( int i = 0, id = 0; i < batches; i++ )
                    {
                        for ( int b = 0; b < batchSize; b++, id++ )
                        {
                            writer.write( labelChanges( id, EMPTY_LONG_ARRAY, someLabels() ) );
                        }
                        progress.add( batchSize );
                    }
                }
            }
        };
    }

    private static long[] someLabels()
    {
        return new long[]{1L, 2L};
    }
}

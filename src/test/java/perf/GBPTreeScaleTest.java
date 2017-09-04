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

import org.apache.commons.lang3.mutable.MutableLong;
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
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.textual;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.EMPTY;
import static org.neo4j.test.rule.PageCacheRule.config;

@RunWith( Parameterized.class )
public class GBPTreeScaleTest
{
    @Parameters( name = "size-{0}_txsize-{1}_load-{2}" )
    public static Collection<Object[]> runs()
    {
        Collection<Object[]> runs = new ArrayList<>();
        runs.add( array( 10_000_000, 100, Loads.RANDOM_UNSORTED ) );
        runs.add( array( 10_000_000, 100, Loads.RANDOM_SORTED ) );
        runs.add( array( 10_000_000, 100, Loads.SEQUENTIAL ) );
        runs.add( array( 10_000_000, 100, Loads.SEQUENTIAL_BACKWARDS ) );
        return runs;
    }

    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule( config().withInconsistentReads( false ) );
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final TestName testName = new TestName();

    @Parameter( 0 )
    public int count;
    @Parameter( 1 )
    public int txSize;
    @Parameter( 2 )
    public Load load;

    interface Load
    {
        void apply( GBPTree<MutableLong,MutableLong> tree, ProgressListener progress, int count, int txSize,
                Random random ) throws IOException;
    }

    @Test
    public void shouldPutLabelScanStoreUnderLoad() throws Exception
    {
        // GIVEN
        GBPTree<MutableLong,MutableLong> tree = new GBPTree<>(
                pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() ),
                testDirectory.file( "tree" ),
                new SimpleLongLayout( "meh" ), 0, GBPTree.NO_MONITOR );

        // WRITE
        long time = currentTimeMillis();
        ProgressListener progress = textual( System.out ).singlePart( "Insert:" + testName.getMethodName(), count );
        load.apply( tree, progress, count, txSize, random.random() );
        long writeTime = currentTimeMillis() - time;
        System.out.println( "write:" + duration( writeTime ) );

        tree.close();

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

    enum Stores implements BiFunction<Supplier<PageCache>,File,LabelScanStore>
    {
        NATIVE
        {
            @Override
            public LabelScanStore apply( Supplier<PageCache> pageCache, File storeDir )
            {
                return new NativeLabelScanStore( pageCache.get(), storeDir, EMPTY, false,
                        LabelScanStore.Monitor.EMPTY );
            }
        };
    }
//
//    protected static long nextRandomNumber( long number, long modifier )
//    {
//
//    }

    enum Loads implements Load
    {
        RANDOM_UNSORTED
        {
            @Override
            public void apply( GBPTree<MutableLong,MutableLong> tree, ProgressListener progress, int count, int txSize,
                    Random random ) throws IOException
            {
                int txs = count / txSize;
                MutableLong key = new MutableLong();
                for ( int t = 0; t < txs; t++ )
                {
                    try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                    {
                        for ( int i = 0; i < txSize; i++ )
                        {
                            key.setValue( random.nextInt( 100_000_000 ) );
                            writer.put( key, key );
                        }
                    }
                    progress.add( txSize );
                }
            }
        },
        RANDOM_SORTED
        {
            @Override
            public void apply( GBPTree<MutableLong,MutableLong> tree, ProgressListener progress, int count, int txSize,
                    Random random ) throws IOException
            {
                int txs = count / txSize;
                long[] ids = new long[txSize];
                MutableLong key = new MutableLong();
                for ( int t = 0; t < txs; t++ )
                {
                    for ( int i = 0; i < txSize; i++ )
                    {
                        ids[i] = random.nextInt( 100_000_000 );
                    }
                    Arrays.sort( ids );
                    try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                    {
                        for ( int i = 0; i < ids.length; i++ )
                        {
                            key.setValue( ids[i] );
                            writer.put( key, key );
                        }
                    }
                    progress.add( txSize );
                }
            }
        },
        SEQUENTIAL
        {
            @Override
            public void apply( GBPTree<MutableLong,MutableLong> tree, ProgressListener progress, int count, int txSize,
                    Random random ) throws IOException
            {
                int txs = count / txSize;
                MutableLong key = new MutableLong();
                long id = 0;
                for ( int t = 0; t < txs; t++ )
                {
                    try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                    {
                        for ( int i = 0; i < txSize; i++, id++ )
                        {
                            key.setValue( id );
                            writer.put( key, key );
                        }
                    }
                    progress.add( txSize );
                }
            }
        },
        SEQUENTIAL_BACKWARDS
        {
            @Override
            public void apply( GBPTree<MutableLong,MutableLong> tree, ProgressListener progress, int count, int txSize,
                    Random random ) throws IOException
            {
                int txs = count / txSize;
                MutableLong key = new MutableLong();
                long id = count - 1;
                for ( int t = 0; t < txs; t++ )
                {
                    try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                    {
                        for ( int i = 0; i < txSize; i++, id-- )
                        {
                            key.setValue( id );
                            writer.put( key, key );
                        }
                    }
                    progress.add( txSize );
                }
            }
        };
    }
}

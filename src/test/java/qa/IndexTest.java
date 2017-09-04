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
package qa;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.neo4j.cursor.Cursor;
import org.neo4j.index.Hit;
import org.neo4j.index.bptree.BPTreeIndex;
import org.neo4j.index.bptree.path.PathIndexLayout;
import org.neo4j.index.bptree.path.SCIndexDescription;
import org.neo4j.index.bptree.path.TwoLongs;
import org.neo4j.index.Modifier;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

import static org.junit.Assert.fail;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class IndexTest
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();
    private PageCache pageCache;
    private File indexFile;
    private final SCIndexDescription description = new SCIndexDescription( "a", "b", "c", OUTGOING, "d", null );
    @SuppressWarnings( "rawtypes" )
    private BPTreeIndex index;

    @SuppressWarnings( "unchecked" )
    public <KEY,VALUE> BPTreeIndex createIndex( int pageSize ) throws IOException
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        pageCache = new MuninnPageCache( swapperFactory, 100, pageSize, NULL );
        indexFile = folder.newFile( "index" );
//        File metaFile = folder.newFile( "meta" );
        return index = new BPTreeIndex( pageCache, indexFile, new PathIndexLayout(), description, pageSize );
    }

    @After
    public void closePageCache() throws IOException
    {
        index.close();
        pageCache.close();
    }

    @Test
    public void shouldSplitCorrectly() throws Exception
    {
        try
        {
            // GIVEN
            BPTreeIndex index = createIndex( 128 );

            // WHEN
            long seed = currentTimeMillis();
            System.out.println( "Seed:" + seed );
            Random random = new Random( seed );
            try ( Modifier inserter = index.inserter() )
            {
                for ( int i = 0; i < 300_000; i++ )
                {
                    if ( (i % 1_000) == 0 )
                    {
                        System.out.println( "insert:" + i );
                    }
//                    long[] key = new long[] {random.nextInt( 10_000_000 ), 0};
//                    long[] value = new long[2];
                    TwoLongs key = new TwoLongs( random.nextInt( 10_000_000 ), 0 );
                    TwoLongs value = new TwoLongs();
                    inserter.insert( key, value );
                }
            }
            index.printTree();

            // THEN
//            {
//                List<SCResult> results = new ArrayList<>();
//   //        index.seek( new Scanner(), results );
//                index.seek( new RangeSeeker( RangePredicate.greaterOrEqual( 0, 0 ), RangePredicate.lower( Long.MAX_VALUE, 0 ) ), results );
//                System.out.println( "size:" + results.size() );
//                long[] prev = new long[] {-1, -1};
//                for ( SCResult result : results )
//                {
//                    long first = result.getKey().getId();
//                    if ( first < prev[0] )
//                    {
//                        index.printTree();
//                        fail( result.getKey() + " smaller than prev " + prev );
//                    }
//                    prev[0] = first;
//                    prev[1] = result.getKey().getProp();
//                }
//            }

//        try ( Cursor<BTreeHit> cursor = index.seek( RangePredicate.greaterOrEqual( 0, 0 ), RangePredicate.lower( Long.MAX_VALUE, 0 ) ) )
//        {
//            long[] prev = new long[] {-1, -1};
//            while ( cursor.next() )
//            {
//                long[] hit = cursor.get().key();
//                if ( hit[0] < prev[0] )
//                {
//                    index.printTree();
//                    fail( hit + " smaller than prev " + prev );
//                }
//                prev[0] = hit[0];
//                prev[1] = hit[1];
//            }
//        }

        try ( Cursor<Hit<TwoLongs,TwoLongs>> cursor = index.seek( new TwoLongs( 0, 0 ), new TwoLongs( Long.MAX_VALUE, 0 ) ) )
        {
            TwoLongs prev = new TwoLongs( -1, -1 );
            while ( cursor.next() )
            {
                TwoLongs hit = cursor.get().key();
                if ( hit.first < prev.first )
                {
                    index.printTree();
                    fail( hit + " smaller than prev " + prev );
                }
                prev = new TwoLongs( hit.first, hit.other );
            }
        }

            System.out.println( "SUCCESS" );
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            throw t;
        }

    }
}

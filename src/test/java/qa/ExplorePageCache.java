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

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static java.nio.file.StandardOpenOption.CREATE;

import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class ExplorePageCache
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( getClass() );

    @Test
    public void shouldExplore1() throws Exception
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        try ( PageCache pageCache = new MuninnPageCache( swapper, 100, 1024, NULL );
              PagedFile pagedFile = pageCache.map( directory.file( "bla" ), pageCache.pageSize(), CREATE ) )
        {
            try ( PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                writeCursor.next();
                writeCursor.setOffset( 0 );
                writeCursor.putInt( 1 );
                writeCursor.putInt( 2 );
            }

            CountDownLatch latch1 = new CountDownLatch( 1 );
            CountDownLatch latch2 = new CountDownLatch( 1 );
            Thread thread = new Thread()
            {
                @Override
                public void run()
                {
                    try ( PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
                    {
                        writeCursor.next();
                        writeCursor.setOffset( 0 );
                        writeCursor.putInt( 4 );

                        latch1.countDown();
                        latch2.await();

                        writeCursor.putInt( 5 );
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                }
            };

            try ( PageCursor readCursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                readCursor.next();
                int first, second;
                int loop = 0;
                do
                {
                    readCursor.setOffset( 0 );
                    first = readCursor.getInt();

                    if ( loop++ == 0 )
                    {
                        thread.start();
                        latch1.await();
                    }

                    second = readCursor.getInt();

                    latch2.countDown();
                }
                while ( readCursor.shouldRetry() );
                assertTrue( loop > 1 );
                assertEquals( second, first + 1 );
            }
        }
    }

    @Test
    public void shouldExplore2() throws Exception
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        try ( PageCache pageCache = new MuninnPageCache( swapper, 100, 1024, NULL );
                PagedFile pagedFile = pageCache.map( directory.file( "bla" ), pageCache.pageSize(), CREATE ) )
        {
            try ( PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                writeCursor.next();
                writeCursor.setOffset( 0 );
                writeCursor.putInt( 1 );
                writeCursor.putInt( 2 );
            }

            CountDownLatch latch1 = new CountDownLatch( 1 );
            CountDownLatch latch2 = new CountDownLatch( 1 );
            Thread thread = new Thread()
            {
                @Override
                public void run()
                {
                    try ( PageCursor readCursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                    {
                        readCursor.next();
                        int first, second;
                        do
                        {
                            System.out.println( "loop" );
                            readCursor.setOffset( 0 );
                            first = readCursor.getInt();
                            second = readCursor.getInt();
                        }
                        while ( readCursor.shouldRetry() );
                        assertEquals( second, first + 1 );
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                }
            };

            try ( PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                writeCursor.next();
                writeCursor.setOffset( 0 );
                writeCursor.putInt( 4 );

                thread.start();
                Thread.sleep( 1_000 );

                writeCursor.putInt( 5 );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void exploreCopyTo() throws Exception
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        try ( PageCache pageCache = new MuninnPageCache( swapper, 100, 1024, NULL );
                PagedFile pagedFile = pageCache.map( directory.file( "bla" ), pageCache.pageSize(), CREATE ) )
        {
            try ( PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                writeCursor.next();
                writeCursor.setOffset( 0 );
                // put 11..20 at pos 0..9
                for ( int i = 11; i <= 20; i++ )
                {
                    writeCursor.putInt( i );
                }
            }

            try ( PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                writeCursor.next();
                // move 11..20 to pos 1..10
                writeCursor.copyTo( 0, writeCursor, 4, 10*4 );
                // put 10 at pos 0
                writeCursor.putInt( 0, 10 );
            }

            try ( PageCursor readCursor = pagedFile.io( 1, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                readCursor.next();
                int[] yo = new int[11];
                do
                {
                    for ( int i = 0; i < yo.length; i++ )
                    {
                        yo[i] = readCursor.getInt();
                    }
                }
                while ( readCursor.shouldRetry() );

                for ( int i = 0; i < yo.length; i++ )
                {
                    assertEquals( 10+i, yo[i] );
                }
            }
        }
    }
}

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

import java.io.File;
import java.nio.file.StandardOpenOption;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;

public class PageCacheCopyVsTmpTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule( false );

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        File file = directory.file( "name" );
        PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );

        // WHEN
        try ( PagedFile pagedFile = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE );
                PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();

            byte[] tmp = new byte[pageCache.pageSize()];
            int itemSize = 10;
            int keyCount = (pageCache.pageSize() / itemSize) - 1;
            int pos = keyCount - 2;
            long time = currentTimeMillis();
            for ( int i = 0; i < 3_000_000; i++ )
            {
//                moveUsingCopyTo( cursor, itemSize, keyCount, pos );
                moveUsingTmp( cursor, itemSize, keyCount, pos, tmp );

                cursor.putLong( pos * itemSize, i );
            }
            time = currentTimeMillis() - time;
            System.out.println( time );
        }
    }

    private void moveUsingTmp( PageCursor cursor, int itemSize, int keyCount, int pos, byte[] tmp )
    {
        // cursor --> tmp
        int count = keyCount - pos;
        cursor.setOffset( pos * itemSize );
        cursor.getBytes( tmp, 0, count * itemSize );

        // tmp --> cursor
        cursor.setOffset( pos * itemSize );
        cursor.putBytes( tmp, 0, count * itemSize );
    }

    private void moveUsingCopyTo( PageCursor cursor, int itemSize, int keyCount, int pos )
    {
        int baseOffset = 0;
        for ( int posToMoveRight = keyCount - 1, offset = baseOffset + posToMoveRight * itemSize;
                posToMoveRight >= pos; posToMoveRight--, offset -= itemSize )
        {
            cursor.copyTo( offset, cursor, offset + itemSize, itemSize );
        }
    }
}

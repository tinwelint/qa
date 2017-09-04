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

import org.junit.Rule;
import org.junit.Test;

import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.test.rule.PageCacheRule.config;

public class TreeNodePageCursorDeltaTest
{
    private static final int itemSize = 32;
    private static final int headerSize = 4;

    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule( config().withInconsistentReads( false ) );
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
        PagedFile file = pageCache.map( directory.file( "file" ), pageCache.pageSize(), StandardOpenOption.CREATE );
        int maxItems = (file.pageSize() - headerSize) / itemSize;

        // WHEN
        Node node =
                new WithDelta( 20 );
//                new SlotPerSlot();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int stride = random.nextInt( maxItems - 1 ) + 1;
        try ( PageCursor cursor = file.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            for ( int i = 0; i < maxItems; i++ )
            {
                node.insert( cursor, i, i == 0 ? 0 : (i + stride) % i );
            }
        }
        file.close();
    }

    interface Node
    {
        void insert( PageCursor cursor, int keyCount, int slot );
    }

    /**
     * [1235678]
     *
     * insert 4:
     *
     * [123 5678]
     * [12345678]
     */
    static class SlotPerSlot implements Node
    {
        @Override
        public void insert( PageCursor cursor, int keyCount, int slot )
        {
            insertSlotAt( cursor, keyCount, slot, 0 );
            cursor.putInt( slot * itemSize, slot ); // only 4B but doesn't matter so much
        }
    }

    private static void insertSlotAt( PageCursor cursor, int keyCount, int slot, int baseOffset )
    {
        for ( int posToMoveRight = keyCount - 1, offset = baseOffset + posToMoveRight * itemSize;
                posToMoveRight >= slot; posToMoveRight--, offset -= itemSize )
        {
            cursor.copyTo( offset, cursor, offset + itemSize, itemSize );
        }
    }

    /**
     * [1235678]
     *
     * insert 4:
     *
     * [1235678          |4   ]
     *
     * insert 9
     *
     * [1235678          |49  ]
     *
     * ...
     *
     * insert ? (consolidates batch-wise)
     *
     * [123456789AB...]
     *
     */
    static class WithDelta implements Node
    {
        private final int maxDeltaSize;
        private final int baseDeltaSectionOffset;

        public WithDelta( int deltaSize )
        {
            this.maxDeltaSize = deltaSize;
            this.baseDeltaSectionOffset = headerSize + maxDeltaSize * itemSize;
        }

        @Override
        public void insert( PageCursor cursor, int keyCount, int slot )
        {
            if ( slot >= keyCount - 1 )
            {
                // first on node or last one, just append right there
                cursor.putInt( headerSize + slot * itemSize, slot );
            }
            else
            {
                // Place in delta section
                int deltaSize = cursor.getInt( 0 ); // header
                if ( deltaSize == maxDeltaSize )
                {
                    // Consolidate delta into node
                    cursor.putInt( 0, 0 ); // Reset deltaSize count in header
                }

                // Add to delta section
                // Brute-force scan through delta section to put it sorted (could be made more efficient)
                cursor.setOffset( baseDeltaSectionOffset );
                for ( int i = 0; i < deltaSize; i++ )
                {
                    int item = cursor.getInt();
                }
            }
        }
    }
}

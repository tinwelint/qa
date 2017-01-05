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

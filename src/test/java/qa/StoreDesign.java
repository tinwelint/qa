package qa;

import org.junit.Test;

import static org.junit.Assert.fail;

public class StoreDesign
{
    @Test
    public void shouldWork() throws Exception
    {
        // GIVEN
        Store<NodeCursor> store = null;

        // WHEN
        try ( NodeCursor cursor = store.cursor() )
        {
            if ( cursor.next( 10 ) )
            {
                long nextRel = cursor.nextRel();
            }
        }

        // THEN
        fail( "Test not fully implemented yet" );
    }

    public interface Cursor extends AutoCloseable
    {
        long id();

        boolean next();

        boolean next( long id );

        @Override
        void close();
    }

    public interface NodeCursor extends Cursor
    {
        long nextRel();

        long nextProp();

        long labels();
    }

    public interface Store<CURSOR extends NodeCursor>
    {
        CURSOR cursor();
    }
}

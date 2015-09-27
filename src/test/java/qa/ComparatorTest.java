package qa;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ComparatorTest
{
    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        List<Long> yo = new ArrayList<>();
        yo.add( 5L );
        yo.add( 2L );
        yo.add( 10L );

        Collections.sort( yo, new Comparator<Long>()
        {
            @Override
            public int compare( Long o1, Long o2 )
            {
                return -o1.compareTo( o2 );
            }
        } );

        for ( long l : yo )
        {
            System.out.println( l );
        }
    }
}

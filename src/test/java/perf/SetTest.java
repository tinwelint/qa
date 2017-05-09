package perf;

import org.junit.Test;
import qa.perf.Operation;
import qa.perf.Operations;
import qa.perf.Performance;
import qa.perf.Target;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.asSet;

public class SetTest
{
    public Object last;

    @Test
    public void shouldTest() throws Exception
    {
        for ( int i = 0; i < 3; i++ )
        {
            Performance.measure( new Types() ).withDuration( 10 ).withOperations( Operations.single( new ToSet() ) )
                    .please();
            Performance.measure( new Types() ).withDuration( 10 ).withOperations( Operations.single( new Dedup() ) )
                    .please();
        }
        System.out.println( last );
    }

    private static class Types implements Target
    {
        private final int[] data;

        Types()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            data = new int[random.nextInt( 1, 5 )];
            for ( int i = 0; i < data.length; i++ )
            {
                data[i] = random.nextInt( 65000 );
            }
        }

        @Override
        public void start() throws Exception
        {
        }

        @Override
        public void stop()
        {
        }
    }

    public class ToSet implements Operation<Types>
    {
        @Override
        public void perform( Types on )
        {
            last = asSet( on.data, t -> t >= 0 );
        }
    }

    public class Dedup implements Operation<Types>
    {
        @Override
        public void perform( Types on )
        {
            last = PrimitiveIntCollections.deduplicate( on.data );
        }
    }
}

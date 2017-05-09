package perf;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;

@RunWith( Parameterized.class )
public class NumberArrayPerf
{
    private static final int COUNT = (int) mebiBytes( 100 );
    private static final int CHUNKS = 100;

    enum Arrays implements Supplier<WrappingArray>
    {
        HEAP_STATIC_LONG( () -> new WrappingLongArray( HEAP.newLongArray( COUNT, 0 ) ) ),
        OFF_HEAP_STATIC_LONG( () -> new WrappingLongArray( OFF_HEAP.newLongArray( COUNT, 0 ) ) ),
        HEAP_DYNAMIC_LONG( () -> new WrappingLongArray( HEAP.newDynamicLongArray( COUNT / CHUNKS, 0 ) ) ),
        OFF_HEAP_DYNAMIC_LONG( () -> new WrappingLongArray( OFF_HEAP.newDynamicLongArray( COUNT / CHUNKS, 0 ) ) );

        private final Supplier<WrappingArray> arrayFactory;

        Arrays( Supplier<WrappingArray> arrayFactory )
        {
            this.arrayFactory = arrayFactory;
        }

        @Override
        public WrappingArray get()
        {
            return arrayFactory.get();
        }
    }

    enum Values implements Supplier<PrimitiveLongIterator>
    {
        SEQUENTIAL( () -> sequential( COUNT ) ),
        RANDOM( () -> random( COUNT ) );

        private final Supplier<PrimitiveLongIterator> valueFactory;

        Values( Supplier<PrimitiveLongIterator> valueFactory )
        {
            this.valueFactory = valueFactory;
        }

        @Override
        public PrimitiveLongIterator get()
        {
            return valueFactory.get();
        }
    }

    @Parameters
    public static final Collection<Object[]> data()
    {
        Collection<Object[]> result = new ArrayList<>();
        for ( Arrays array : Arrays.values() )
        {
            for ( Values value : Values.values() )
            {
                result.add( array( array, value ) );
            }
        }
        return result;
    }

    private static PrimitiveLongIterator random( long count )
    {
        long seed = ThreadLocalRandom.current().nextLong();
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private long cursor;
            private long next = seed;

            @Override
            protected boolean fetchNext()
            {
                if ( cursor >= count )
                {
                    return false;
                }

                cursor++;
                return next( next = ((next + seed) & (COUNT - 1)) );
            }

            @Override
            public String toString()
            {
                return "RANDOM " + count;
            }
        };
    }

    private static PrimitiveLongIterator sequential( long count )
    {
        return new PrimitiveLongCollections.PrimitiveLongRangeIterator( 0, count - 1, 1 )
        {
            @Override
            public String toString()
            {
                return "SEQUENTIAL " + count;
            }
        };
    }

    @Parameter( value = 0 )
    public Supplier<WrappingArray> arrayFactory;
    @Parameter( value = 1 )
    public Supplier<PrimitiveLongIterator> valueFactory;

    @Test
    public void shouldTest() throws Exception
    {
        // WHEN
        long time = 0;
        for ( int i = 0; i < 3; i++ )
        {
            try ( WrappingArray array = arrayFactory.get() )
            {
                PrimitiveLongIterator values = valueFactory.get();
                time = currentTimeMillis();
                while ( values.hasNext() )
                {
                    array.set( values.next(), 100 );
                }
                time = currentTimeMillis() - time;
            }
        }

        // THEN
        System.out.println( arrayFactory + " " + valueFactory + ": " + duration( time ) );
    }

    private static abstract class WrappingArray implements AutoCloseable
    {
        abstract void set( long index, long value );

        abstract long get( long index );

        @Override
        public abstract void close();
    }

    private static class WrappingLongArray extends WrappingArray
    {
        private final LongArray actual;

        WrappingLongArray( LongArray actual )
        {
            this.actual = actual;
        }

        @Override
        public void set( long index, long value )
        {
            this.actual.set( index, value );
        }

        @Override
        public long get( long index )
        {
            return actual.get( index );
        }

        @Override
        public void close()
        {
            actual.close();
        }
    }
}

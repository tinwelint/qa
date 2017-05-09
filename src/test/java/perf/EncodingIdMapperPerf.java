package perf;

import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Radix;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.StringEncoder;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.TrackerFactories;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.SimpleInputIteratorWrapper;

import static org.mockito.Mockito.mock;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.duration;

public class EncodingIdMapperPerf
{
    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        EncodingIdMapper mapper = new EncodingIdMapper( NumberArrayFactory.AUTO, new StringEncoder(), Radix.STRING,
                EncodingIdMapper.NO_MONITOR, TrackerFactories.dynamic() );
        InputIterable<Object> values = SimpleInputIteratorWrapper.wrap( "yo", new Iterable<Object>()
        {
            @Override
            public Iterator<Object> iterator()
            {
                return new PrefetchingIterator<Object>()
                {
                    private int i;

                    @Override
                    protected Object fetchNextOrNull()
                    {
                        return String.valueOf( i++ );
                    }
                };
            }
        } );
        InputIterator<Object> iterator = values.iterator();
        long maxId = 1L << 26;
        System.out.println( maxId );
        for ( int i = 0; i < maxId; i++ )
        {
            mapper.put( iterator.next(), i, Group.GLOBAL );
        }
        System.out.println( "All put" );
        mapper.prepare( values, mock( Collector.class ), ProgressListener.NONE );
        System.out.println( "Prepared" );

        // WHEN
        long seed = ThreadLocalRandom.current().nextLong( 100, Long.MAX_VALUE );
        long time = currentTimeMillis();
        long id = seed;
        long mask = maxId - 1;
        for ( int i = 0; i < maxId; i++ )
        {
            mapper.get( String.valueOf( id ), Group.GLOBAL );
            id = (id + seed) & mask;
        }
        time = currentTimeMillis() - time;

        // THEN
        System.out.println( duration( time ) );
    }
}

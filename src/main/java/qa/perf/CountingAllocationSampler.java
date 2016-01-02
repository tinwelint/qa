package qa.perf;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class CountingAllocationSampler implements AllocationSampler
{
    private final ConcurrentMap<String,AtomicLong> counts = new ConcurrentHashMap<>();

    @Override
    public void sampleAllocation( int count, String desc, Object newObj, long size )
    {
        AtomicLong c = counts.get( desc );
        if ( c == null )
        {
            counts.putIfAbsent( desc, new AtomicLong() );
            c = counts.get( desc );
        }
        c.incrementAndGet();
    }

    @Override
    public void close( long totalOps )
    {
        Map.Entry<String,AtomicLong>[] entries = counts.entrySet().toArray( new Map.Entry[counts.size()] );
        Arrays.sort( entries, new Comparator<Map.Entry<String,AtomicLong>>()
        {
            @Override
            public int compare( Entry<String,AtomicLong> o1, Entry<String,AtomicLong> o2 )
            {
                int comparison = Long.compare( o2.getValue().get(), o1.getValue().get() );
                if ( comparison != 0 )
                {
                    return comparison;
                }

                return o1.getKey().compareTo( o2.getKey() );
            }
        } );

        long total = 0;
        for ( Entry<String,AtomicLong> entry : entries )
        {
            System.out.println( entry );
            total += entry.getValue().get();
        }
        System.out.println( "TOTAL: " + total + " (" + ((double)total/totalOps) + " objects/op)" );
    }
}

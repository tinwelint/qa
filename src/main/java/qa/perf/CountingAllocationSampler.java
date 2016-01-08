package qa.perf;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class CountingAllocationSampler implements AllocationSampler
{
    private static final int GUTTER_SIZE = 6;
    private final ConcurrentMap<String,AtomicLong> counts = new ConcurrentHashMap<>();
    private final double divisibilityPrintThreshold;

    public CountingAllocationSampler( double divisibilityPrintThreshold )
    {
        this.divisibilityPrintThreshold = divisibilityPrintThreshold;
    }

    @Override
    public void sampleAllocation( int count, String desc, Object newObj, long size )
    {
        String key = desc;
        if ( count != -1 )
        {
            key += "[]";
        }
        AtomicLong c = counts.get( key );
        if ( c == null )
        {
            counts.putIfAbsent( key, new AtomicLong() );
            c = counts.get( key );
        }
        c.incrementAndGet();
    }

    @Override
    public void close( long totalOps )
    {
        @SuppressWarnings( "unchecked" )
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

        long total = countTotal( entries );
        System.out.println( "TOTAL: " + total + " (" + ((double)total/totalOps) + " objects/op)" );
        long notPrinted = 0;
        for ( Entry<String,AtomicLong> entry : entries )
        {
            long count = entry.getValue().get();
            double divisibility = (double)count / (double)totalOps;
            if ( divisibility >= divisibilityPrintThreshold )
            {
                String tab = "";
                if ( divisibility % 1 == 0 )
                {
                    tab = "x" + (int) divisibility;
                }
                System.out.println( pad( tab, GUTTER_SIZE ) + entry );
            }
            else
            {
                notPrinted += count;
            }
            total += count;
        }

        System.out.println( pad( format(
                "    ... %d (e.g. %f%% of all objects) not printed", notPrinted, 100d * notPrinted / total ), 6 ) );
    }

    private String pad( String string, int to )
    {
        while ( string.length() < to )
        {
            string += " ";
        }
        return string;
    }

    private long countTotal( Entry<String,AtomicLong>[] entries )
    {
        long total = 0;
        for ( Entry<String,AtomicLong> entry : entries )
        {
            total += entry.getValue().get();
        }
        return total;
    }
}

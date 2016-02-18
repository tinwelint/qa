package qa.perf;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.lang.String.format;

public class CountingAllocationSampler implements AllocationSampler
{
    private static final int GUTTER_SIZE = 6;
    private static final Comparator<Map.Entry<String,AtomicLong>> SORTER =
            new Comparator<Map.Entry<String,AtomicLong>>()
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
    };

    private final ConcurrentMap<String,AtomicLong> counts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String,AtomicLong> ignored = new ConcurrentHashMap<>();
    private final double multiplierPrintThreshold;
    private final boolean verbose;
    private final Predicate<StackTraceElement[]>[] callStackFilters;
    private final Predicate<String> threadFilter;

    /**
     * @param multiplierPrintThreshold print allocations down to this multiplier of totalOps.
     * So if totalOps is 30 then object counts from 30 and up will be printed, the rest ignored,
     * in {@link #close(long)}.
     * @param verbose whether or not to print verbose information.
     * @param callStackFilters {@link Collection} of {@link Predicate}, where if any says {@code false}
     * then that allocation will be ignored.
     * @param threadFilter {@link Predicate} for which threads to register allocations from.
     */
    @SuppressWarnings( "unchecked" )
    public CountingAllocationSampler( double multiplierPrintThreshold, boolean verbose,
            Collection<Predicate<StackTraceElement[]>> callStackFilters,
            Predicate<String> threadFilter )
    {
        this.multiplierPrintThreshold = multiplierPrintThreshold;
        this.verbose = verbose;
        this.callStackFilters = callStackFilters.toArray( new Predicate[callStackFilters.size()] );
        this.threadFilter = threadFilter;
    }

    @Override
    public void sampleAllocation( int count, String desc, Object newObj, long size )
    {
        if ( !threadFilter.test( Thread.currentThread().getName() ) )
        {
            return;
        }
        ConcurrentMap<String,AtomicLong> map = counts;
        String key = desc;
        if ( count != -1 )
        {
            key += "[]";
        }
        if ( callStackFilters.length > 0 )
        {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            for ( Predicate<StackTraceElement[]> filter : callStackFilters )
            {
                if ( !filter.test( stack ) )
                {
                    map = ignored;
                    key = filter.toString();
                    break;
                }
            }
        }

        AtomicLong c = map.get( key );
        if ( c == null )
        {
            map.putIfAbsent( key, new AtomicLong() );
            c = map.get( key );
        }
        c.incrementAndGet();
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public void close( long totalOps )
    {
        Map.Entry<String,AtomicLong>[] entries = counts.entrySet().toArray( new Map.Entry[counts.size()] );
        long total = countTotal( entries );
        printTotal( "ALLOCATED", total, totalOps );

        if ( verbose )
        {
            Arrays.sort( entries, SORTER );
            long notPrinted = 0;
            for ( Entry<String,AtomicLong> entry : entries )
            {
                long count = entry.getValue().get();
                double divisibility = (double)count / (double)totalOps;
                if ( divisibility >= multiplierPrintThreshold )
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
            }

            System.out.println( pad( "", 6 ) +
                    format( "... %d (e.g. %f%% of all objects) not printed", notPrinted, 100d * notPrinted / total ) );

            // Print ignored by call stack filtering
            if ( !ignored.isEmpty() )
            {
                Map.Entry<String,AtomicLong>[] ignoredEntries = ignored.entrySet().toArray( new Map.Entry[ignored.size()] );
                printTotal( "IGNORED", countTotal( ignoredEntries ), totalOps );
                Arrays.sort( ignoredEntries, SORTER );
                for ( Entry<String,AtomicLong> entry : ignoredEntries )
                {
                    System.out.println( pad( "", GUTTER_SIZE ) + entry.getKey() + " " + entry.getValue() );
                }
            }
        }
    }

    private void printTotal( String type, long total, long totalOps )
    {
        System.out.println( type + ": " + total + " (" + ((double)total/totalOps) + " objects/op)" );
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

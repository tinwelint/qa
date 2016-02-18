/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package qa.perf;

import com.google.monitoring.runtime.instrumentation.AllocationRecorder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static qa.perf.Operations.single;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Performance<T extends Target>
{
    private static final String THREAD_NAME_PREFIX = "Performance-Worker-";
    private final T target;
    private Operation<T> initial;
    private Supplier<Operation<T>> operations;
    private int batchSize = 50;
    private int threads = 1;
    private long durationSeconds = 5;
    private String name;
    private int warmupOps = 1;
    private boolean allocationSampling;
    private boolean verboseAllocationSampling;
    private final Collection<Predicate<StackTraceElement[]>> allocationFiltering = new ArrayList<>();

    private Performance( T target )
    {
        this.target = target;
    }

    public static <T extends Target> Performance<T> measure( T target )
    {
        return new Performance<>( target );
    }

    public Performance<T> withInitialOperation( Operation<T> initial )
    {
        this.initial = initial;
        return this;
    }

    public Performance<T> withOperations( Operation<T> operations )
    {
        return withOperations( single( operations ) );
    }

    public Performance<T> withOperations( Supplier<Operation<T>> operations )
    {
        this.operations = operations;
        return this;
    }

    public Performance<T> withBatchSize( int batchSize )
    {
        this.batchSize = batchSize;
        return this;
    }

    public Performance<T> withThreads( int thread )
    {
        this.threads = thread;
        return this;
    }

    public Performance<T> withAllCores()
    {
        return withThreads( Runtime.getRuntime().availableProcessors() );
    }

    public Performance<T> withDuration( long seconds )
    {
        this.durationSeconds = seconds;
        return this;
    }

    public Performance<T> withAllocationSampling()
    {
        this.allocationSampling = true;
        this.verboseAllocationSampling = false;
        return this;
    }

    public Performance<T> withVerboseAllocationSampling()
    {
        this.allocationSampling = true;
        this.verboseAllocationSampling = true;
        return this;
    }

    public Performance<T> withAllocationSamplingCallStackExclusionByPackage( String pkg )
    {
        this.allocationFiltering.add( new Predicate<StackTraceElement[]>()
        {
            @Override
            public boolean test( StackTraceElement[] callStack )
            {
                try
                {
                    boolean contains = false;
                    for ( StackTraceElement element : callStack )
                    {
                        String elementPkg = Class.forName( element.getClassName() ).getPackage().getName();
                        if ( elementPkg.startsWith( pkg ) )
                        {
                            contains = true;
                            break;
                        }
                    }
                    return !contains;
                }
                catch ( Exception e )
                {
                    return true;
                }
            }

            @Override
            public String toString()
            {
                return "Objects allocated from inside " + pkg + ".*";
            }
        } );
        return this;
    }

    public Performance<T> withName( String name )
    {
        this.name = name;
        return this;
    }

    /**
     * @param indirection call stack items away. 0 means the name of the method calling this method.
     */
    public Performance<T> withNameFromCallingMethod( int indirection )
    {
        return withName( Thread.currentThread().getStackTrace()[2+indirection].getMethodName() );
    }

    public Performance<T> withWarmup( int warmupOps )
    {
        this.warmupOps = warmupOps;
        return this;
    }

    public double please() throws Exception
    {
        target.start();
        long totalOps = 0;
        AllocationSampler allocationSampler = null;
        try
        {
            if ( initial != null )
            {
                initial.perform( target );
            }

            for ( int i = 0; i < warmupOps; i++ )
            {
                operations.get().perform( target );
            }

            if ( allocationSampling )
            {
                allocationSampler = new CountingAllocationSampler( 1, verboseAllocationSampling,
                        allocationFiltering, (threadName) -> threadName.startsWith( THREAD_NAME_PREFIX ) );
                AllocationRecorder.addSampler( allocationSampler );
            }
            @SuppressWarnings( "rawtypes" )
            Worker[] workers = new Worker[threads];
            AtomicBoolean end = new AtomicBoolean();
            for ( int i = 0; i < threads; i++ )
            {
                workers[i] = new Worker<>( THREAD_NAME_PREFIX + i, target, operations, batchSize, end );
                workers[i].start();
            }

            long startTime = currentTimeMillis();
            long endTime = startTime + SECONDS.toMillis( durationSeconds );
            while ( currentTimeMillis() < endTime )
            {
                Thread.sleep( 100 );
            }
            end.set( true );

            for ( Worker<T> worker : workers )
            {
                worker.join();
                totalOps += worker.getCompletedCount();
            }
            long actualDuration = currentTimeMillis()-startTime;
            double opsPerMilli = (double) totalOps / (double) actualDuration;
            System.out.println( "====== " + (name != null ? name + ": " : "") + opsPerMilli +
                    " ops/ms (ops=" + totalOps + ")" + " ======" );
            return opsPerMilli;
        }
        finally
        {
            if ( allocationSampler != null )
            {
                allocationSampler.close( totalOps );
            }
            target.stop();
        }
    }

    public static <T extends Target> double measure( T target, Operation<T> initial,
            Supplier<Operation<T>> operation, int threads, long durationSeconds ) throws Exception
    {
        return measure( target, initial, operation, 50, threads, durationSeconds );
    }

    /**
     * @return ops/ms
     */
    public static <T extends Target> double measure( T target, Operation<T> initial,
            Supplier<Operation<T>> operation, int batchSize, int threads, long durationSeconds ) throws Exception
    {
        return new Performance<>( target )
                .withInitialOperation( initial )
                .withOperations( operation )
                .withBatchSize( batchSize )
                .withThreads( threads )
                .withDuration( durationSeconds )
                .please();
    }
}

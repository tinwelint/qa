/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package tooling;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.ProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.Step;
import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

public class DynamicProcessorAssignerTestTool
{
    public static void main( String[] args )
    {
        Configuration config = new Configuration()
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return 100;
            }
        };
        Work work = new Work( config );
        work.setTimes( 10 );
        List<Step<?>> steps = new ArrayList<>();
        Stage stage = new MyStage( config, work, steps );

        new Thread()
        {
            @Override
            public void run()
            {
                while ( true )
                {
                    try
                    {
                        Thread.sleep( 200 );
                        if ( System.in.available() > 0 )
                        {
                            BufferedReader reader = new BufferedReader( new InputStreamReader( System.in ) );
                            String line = reader.readLine();
                            if ( line.equals( "ls" ) )
                            {
                                for ( Step<?> step : steps )
                                {
                                    System.out.println( step.stats().toString( DetailLevel.BASIC ) + " " + step.processors( 0 ) );
                                }
                                System.out.println( work );
                            }
                            else
                            {
                                String[] tokens = line.split( " " );
                                int stepNumber = Integer.parseInt( tokens[0] );
                                int millis = Integer.parseInt( tokens[1] );
                                work.setTime( stepNumber, millis );
                                System.out.println( "[" + stepNumber + "] " + millis );
                            }
                        }
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        superviseDynamicExecution( ExecutionMonitors.defaultVisible(), config, stage );
    }

    private static class MyStage extends Stage
    {
        MyStage( Configuration config, Work work, List<Step<?>> steps )
        {
            super( "Test", config );

            int numberOfSteps = 6;
            AtomicInteger nextNumber = new AtomicInteger();
            add( new ProducerStep( control(), config )
            {
                @Override
                protected void process()
                {
                    while ( true )
                    {
                        long time = nanoTime();
                        Object doWork = work.doWork( 0 );
                        time = nanoTime() - time;
                        sendDownstream( doWork );
                        totalProcessingTime.add( time );
                    }
                }

                @Override
                protected long position()
                {
                    return 0;
                }
            }, steps );
            for ( int i = 0; i < 2; i++ )
            {
                int number = nextNumber.incrementAndGet();
                add( new ProcessorStep<Object>( control(), "Step" + number, config, 0 )
                {
                    @Override
                    protected void process( Object batch, BatchSender sender ) throws Throwable
                    {
                        Object result = work.doWork( number );
                        sender.send( result );
                    }
                }, steps );
            }
            int forkedNumber = nextNumber.incrementAndGet();
            add( new ForkedProcessorStep<Object>( control(), "FORKED" + forkedNumber, config,
                    config.maxNumberOfProcessors() )
            {
                @Override
                protected void forkedProcess( int id, int processors, Object batch )
                {
                    work.doForkedWork( forkedNumber, processors );
                }
            }, steps );
            for ( int i = 0; i < 2; i++ )
            {
                int number = nextNumber.incrementAndGet();
                add( new ProcessorStep<Object>( control(), "Step" + number, config, 0 )
                {
                    @Override
                    protected void process( Object batch, BatchSender sender ) throws Throwable
                    {
                        Object result = work.doWork( number );
                        if ( number < numberOfSteps - 1 )
                        {
                            sender.send( result );
                        }
                    }
                }, steps );
            }
        }

        private void add( Step<?> step, List<Step<?>> to )
        {
            to.add( step );
            add( step );
        }
    }

    private static class Work
    {
        private final Object work = new Object();
        private final int[] times;

        Work( Configuration config )
        {
            times = new int[config.maxNumberOfProcessors()];
        }

        Object doWork( int stepNumber )
        {
            try
            {
                Thread.sleep( times[stepNumber] );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            return work;
        }

        Object doForkedWork( int stepNumber, int processors )
        {
            int millis = times[stepNumber];
            LockSupport.parkNanos( MILLISECONDS.toNanos( millis ) / processors );
            return work;
        }

        public void setTime( int stepNumber, int millis )
        {
            times[stepNumber] = millis;
        }

        public void setTimes( int millis )
        {
            Arrays.fill( times, millis );
        }

        @Override
        public String toString()
        {
            return "times " + Arrays.toString( times );
        }
    }
}

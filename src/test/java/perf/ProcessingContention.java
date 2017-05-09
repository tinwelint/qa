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
package perf;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.string.UTF8;
import org.neo4j.unsafe.impl.batchimport.staging.TicketedProcessing;

public class ProcessingContention
{
    public static void main( String[] args ) throws InterruptedException
    {
//        Thread.sleep( 10_000 );

        int processors = 8;
        BiFunction<Integer,Void,Object> processor = (input,state) ->
        {
            String string = String.valueOf( input );
            return UTF8.encode( string );
        };
        try ( TicketedProcessing<Integer,Void,Object> processing = new TicketedProcessing<>( "YO", processors,
                processor, () -> null, false ) )
        {
            processing.processors( processors - processing.processors( 0 ) );
            processing.slurp( source(), true );

            AtomicLong progress = new AtomicLong();
            ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
            Runnable statPrinter = new Runnable()
            {
                long previous = 0;

                @Override
                public void run()
                {
                    long current = progress.get();
                    long delta = current - previous;
                    System.out.println( "DELTA " + delta );
                    previous = current;
                }
            };
            service.scheduleAtFixedRate( statPrinter, 1, 1, TimeUnit.SECONDS );
            for ( int i = 0; processing.next() != null; i++ )
            {
                if ( i % 10_000 == 0 )
                {
                    progress.set( i );
                }
            }
        }
    }

    private static Iterator<Integer> source()
    {
        return new PrefetchingIterator<Integer>()
        {
            private int i;

            @Override
            protected Integer fetchNextOrNull()
            {
                return i++;
            }
        };
    }
}

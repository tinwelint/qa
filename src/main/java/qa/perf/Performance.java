/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Performance
{
    /**
     * @return ops/ms
     */
    public static <T extends Target> double measure( T target, Operation<T> initial,
            OperationSet<T> operation, int threads, long durationSeconds ) throws Exception
    {
        target.start();
        try
        {
            if ( initial != null )
            {
                initial.perform( target );
            }
            Worker[] workers = new Worker[threads];
            AtomicBoolean end = new AtomicBoolean();
            for ( int i = 0; i < threads; i++ )
            {
                workers[i] = new Worker<>( target, operation, end );
                workers[i].start();
            }

            long startTime = currentTimeMillis();
            long endTime = startTime + SECONDS.toMillis( durationSeconds );
            while ( currentTimeMillis() < endTime )
            {
                Thread.sleep( 1_000 );
            }
            end.set( true );

            long totalCount = 0;
            for ( Worker<T> worker : workers )
            {
                worker.join();
                totalCount += worker.getCompletedCount();
            }
            long actualDuration = currentTimeMillis()-startTime;
            double opsPerMilli = (double) totalCount / (double) actualDuration;
            System.out.println( opsPerMilli + " ops/ms" );
            return opsPerMilli;
        }
        finally
        {
            target.stop();
        }
    }
}

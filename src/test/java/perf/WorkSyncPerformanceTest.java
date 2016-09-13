/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package perf;

import org.junit.Test;
import qa.perf.Operation;
import qa.perf.Performance;
import qa.perf.Target;

import java.util.concurrent.TimeUnit;

import org.neo4j.concurrent.Work;
import org.neo4j.concurrent.WorkSync;

public class WorkSyncPerformanceTest
{
    @Test
    public void shouldMeasure() throws Exception
    {
        Performance.measure( new WorkSyncTarget() )
                .withAllCores()
                .withOperations( addWork() )
                .withDuration( 30 )
                .please();
    }

    private Operation<WorkSyncTarget> addWork()
    {
        return (target) -> target.workSync.apply( new IntegerWork( 1 ) );
    }

    private static class WorkSyncTarget implements Target
    {
        protected final Material material = new Material();
        protected WorkSync<Material,IntegerWork> workSync;

        @Override
        public void start() throws Exception
        {
            workSync = new WorkSync<>( material );
        }

        @Override
        public void stop()
        {
            System.out.println( material.getTotal() );
        }
    }

    private static class Material
    {
        private int total;

        public int getTotal()
        {
            return total;
        }
    }

    private static class IntegerWork implements Work<Material,IntegerWork>
    {
        private final int add;

        public IntegerWork( int add )
        {
            this.add = add;
        }

        @Override
        public IntegerWork combine( IntegerWork work )
        {
            return new IntegerWork( add + work.add );
        }

        @Override
        public void apply( Material material )
        {
            material.total += add;
            usleep( 1 );
        }
    }

    private static void usleep( long micros )
    {
        long deadline = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos( micros );
        long now;
        do
        {
            now = System.nanoTime();
        }
        while ( now < deadline );
    }
}

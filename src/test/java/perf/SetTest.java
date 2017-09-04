/**
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import qa.perf.Operations;
import qa.perf.Performance;
import qa.perf.Target;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.asSet;

public class SetTest
{
    public Object last;

    @Test
    public void shouldTest() throws Exception
    {
        for ( int i = 0; i < 3; i++ )
        {
            Performance.measure( new Types() ).withDuration( 10 ).withOperations( Operations.single( new ToSet() ) )
                    .please();
            Performance.measure( new Types() ).withDuration( 10 ).withOperations( Operations.single( new Dedup() ) )
                    .please();
        }
        System.out.println( last );
    }

    private static class Types implements Target
    {
        private final int[] data;

        Types()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            data = new int[random.nextInt( 1, 5 )];
            for ( int i = 0; i < data.length; i++ )
            {
                data[i] = random.nextInt( 65000 );
            }
        }

        @Override
        public void start() throws Exception
        {
        }

        @Override
        public void stop()
        {
        }
    }

    public class ToSet implements Operation<Types>
    {
        @Override
        public void perform( Types on )
        {
            last = asSet( on.data, t -> t >= 0 );
        }
    }

    public class Dedup implements Operation<Types>
    {
        @Override
        public void perform( Types on )
        {
            last = PrimitiveIntCollections.deduplicate( on.data );
        }
    }
}

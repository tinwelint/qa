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

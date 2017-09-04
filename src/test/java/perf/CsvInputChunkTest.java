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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;

import static java.lang.Math.round;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.IdType.INTEGER;

public class CsvInputChunkTest
{
    @Test
    public void shouldPerformGood() throws Exception
    {
        // GIVEN
        Charset charset = Charset.forName( "ASCII" );
        Collector collector = new BadCollector( System.out, 0, 0 );
        int threads = Runtime.getRuntime().availableProcessors();
        File relationshipsFile = new File( "K:/csv/relationships.csv" );
        File nodesFile = new File( "K:/csv/nodes.csv" );
        Input input = new CsvInput(
                datas( data( NO_DECORATOR, charset, nodesFile ) ), defaultFormatNodeFileHeader(),
                datas( data( NO_DECORATOR, charset, relationshipsFile ) ), defaultFormatRelationshipFileHeader(),
                INTEGER, COMMAS, collector );

        // WHEN
        read( nodesFile.length(), threads, input.nodes() );
        read( relationshipsFile.length(), threads, input.relationships() );
    }

    private void read( long size, int threads, InputIterable inputIterable ) throws InterruptedException
    {
        ExecutorService executor = Executors.newFixedThreadPool( threads );
        InputIterator iterator = inputIterable.iterator();
        long time = currentTimeMillis();
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws IOException
                {
                    InputChunk chunk = iterator.newChunk();
                    InputEntityVisitor visitor = new MyVisitor();
                    while ( iterator.next( chunk ) )
                    {
                        while ( chunk.next( visitor ) );
                    }
                    return null;
                }
            } );
        }
        executor.shutdown();
        executor.awaitTermination( 1, DAYS );
        time = currentTimeMillis() - time;
        System.out.println( "Read " + bytes( size ) + " in " + duration( time ) + " i.e. "
                + bytes( round( size / (time / 1000D) ) ) + "/s" );
    }

    public class MyVisitor extends InputEntityVisitor.Adapter
    {
    }
}

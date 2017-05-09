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

import java.io.File;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.relationshipData;
import static org.neo4j.unsafe.impl.batchimport.input.csv.IdType.ACTUAL;

public class CsvInputPerformanceTest
{
    @Test
    public void shouldReadCsvInputQuickly() throws Exception
    {
        // GIVEN
        Collector collector = new BadCollector( System.out, 0, 0 );
        Decorator<InputNode> nodeDecorator = InputEntityDecorators.additiveLabels( array( "YO" ) );
        int maxProcessors = getRuntime().availableProcessors();
        Input input = new CsvInput(
                datas( data( nodeDecorator, defaultCharset(), new File( "K:\\csv\\nodes.csv" ) ) ),
                defaultFormatNodeFileHeader(),
                relationshipData(),
                defaultFormatRelationshipFileHeader(),
                ACTUAL,
                COMMAS,
                collector,
                maxProcessors );

        // WHEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            nodes.next();
            nodes.processors( maxProcessors );
            long time = currentTimeMillis();
            long count = 0;
            count = Iterators.count( nodes );
            time = currentTimeMillis() - time;
            System.out.println( count / (time/1000D) + " nodes/s" );
        }
    }
}

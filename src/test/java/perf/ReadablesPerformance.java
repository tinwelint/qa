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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.ProcessingSource;
import org.neo4j.csv.reader.Readables;
import org.neo4j.csv.reader.Source.Chunk;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.duration;
import static org.neo4j.io.ByteUnit.mebiBytes;

public class ReadablesPerformance
{
    public static void main( String[] args ) throws IOException
    {
        CharReadable readable = Readables.files( Charset.defaultCharset(), new File( "K:/csv/relationships.csv" ) );
        ProcessingSource source = new ProcessingSource( readable, (int) mebiBytes( 4 ), 1 );
        long time = currentTimeMillis();
        while ( true )
        {
            Chunk chunk = source.nextChunk();
            if ( chunk.length() <= 0 )
            {
                break;
            }
        }
        time = currentTimeMillis() - time;
        System.out.println( duration( time ) );
    }
}

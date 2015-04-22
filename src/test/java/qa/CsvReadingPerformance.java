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
package qa;

import org.junit.Test;

import java.io.File;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Mark;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.csv.reader.BufferedCharSeeker.DEFAULT_QUOTE_CHAR;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.file;

public class CsvReadingPerformance
{
    @Test
    public void shouldReadIt() throws Exception
    {
        // GIVEN
        CharSeeker seeker = charSeeker( file(
                new File( "C:\\Users\\Mattias\\Java\\neo4j\\community\\import-tool\\target\\relationships.zip" ) ),
                DEFAULT_QUOTE_CHAR );
        Mark mark = new Mark();
        int[] delim = new int[] {','};

        // WHEN
        long time = currentTimeMillis();
        while ( seeker.seek( mark, delim ) )
        {
            // Just go, man
        }
        time = currentTimeMillis()-time;
        System.out.println( time );

        // THEN
    }
}

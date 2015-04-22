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


import java.io.File;
import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;

import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.files;

public class CheckLdbcExternalPropertiesOrder
{
    private static int delim = '|';
    private static Extractors extractors = new Extractors( ';' );

    public static void main( String[] args ) throws IOException
    {
        File dir = new File( "C:\\Users\\Mattias\\Work\\datasets\\ldbc\\social_network" );
        File nodes = new File( dir, "person_0.csv" );
        File props = new File( dir, "person_email_emailaddress_0.csv" );

        PrimitiveLongIterator nodeIds = ids( nodes );
        PrimitiveLongIterator propIds = ids( props );

        long propId = -1;
        while ( nodeIds.hasNext() )
        {
            long nodeId = nodeIds.next();
            if ( propId != -1 && propId == nodeId )
            {
                System.out.println( nodeId + " +1" );
                propId = -1;
            }

            // look for it in the properties file
            while ( propIds.hasNext() )
            {
                long id = propIds.next();
                if ( id == nodeId )
                {
                    System.out.println( nodeId + " +more" );
                }
                else
                {
                    propId = id;
                    break;
                }
            }
        }

//        long prevId = 0;
//        while ( propIds.hasNext() )
//        {
//            long id = propIds.next();
//            if ( id == prevId )
//            {
//                continue;
//            }
//            prevId = id;
//
//            if ( !nodeIds.hasNext() )
//            {
//                System.out.println( "node file ran out" );
//            }
//
//            int skipped = 0;
//            while ( nodeIds.hasNext() )
//            {
//                long nodeId = nodeIds.next();
//                if ( nodeId == id )
//                {
//                    System.out.println( id + " found after " + skipped + " skipped" );
//                    break;
//                }
//                if ( ((++skipped) % 10) == 0 )
//                {
//                    System.out.println( "still looking for " + id + ", at " + skipped + " skipped" );
//                }
//            }
//        }
    }

    private static PrimitiveLongIterator ids( final File file ) throws IOException
    {
        final CharSeeker seeker = charSeeker( files( defaultCharset(), file ), '"' );
        final Mark mark = new Mark();
        skipLine( seeker, mark );
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            @Override
            protected boolean fetchNext()
            {
                try
                {
                    while ( seeker.seek( mark, delim ) )
                    {
                        long id = seeker.extract( mark, extractors.long_() ).longValue();
                        while ( !mark.isEndOfLine() )
                        {
                            seeker.seek( mark, delim );
                        }
                        return next( id );
                    }
                    seeker.close();
                    return false;
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    private static void skipLine( CharSeeker seeker, Mark mark ) throws IOException
    {
        while ( seeker.seek( mark, (char) 0 ) )
        {
            if ( mark.isEndOfLine() )
            {
                break;
            }
        }
    }
}

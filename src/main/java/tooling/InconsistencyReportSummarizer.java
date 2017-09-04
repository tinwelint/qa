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
package tooling;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.FileUtils.LineListener;

public class InconsistencyReportSummarizer
{
    private static final String INDEX_RULE = "IndexRule[id=";
    private static final String PROPERTY_RECORD = "Property[";

    public static void main( String[] args ) throws IOException
    {
        File file = new File( args[0] );
        Map<String,AtomicInteger> counts = new HashMap<>();
        PrimitiveLongSet indexRules = Primitive.longSet();
        PrimitiveLongSet propertyRecords = Primitive.longSet();
        FileUtils.readTextFile( file, new LineListener()
        {
            @Override
            public void line( String line )
            {
                if ( !Character.isWhitespace( line.charAt( 0 ) ) && line.contains( "ERROR" ) )
                {
                    String error = line.substring( line.indexOf( ']' ) + 1, line.length() );
                    counts.computeIfAbsent( error, e -> new AtomicInteger() ).incrementAndGet();
                }
                else if ( line.contains( INDEX_RULE ) )
                {
                    indexRules.add( extractId( line, INDEX_RULE ) );
                }
                else if ( line.contains( PROPERTY_RECORD ) )
                {
                    propertyRecords.add( extractId( line, PROPERTY_RECORD ) );
                }
            }

            private long extractId( String line, String match )
            {
                int beginIndex = line.indexOf( match ) + match.length();
                int endIndex = line.indexOf( ',', beginIndex );
                long id = Long.parseLong( line.substring( beginIndex, endIndex ) );
                return id;
            }
        } );

        System.out.println( "COUNTS" );
        counts.forEach( (error,count) -> System.out.println( count + ": " + error ) );
//        System.out.println( "RULES" );
//        printSorted( indexRules );
//        indexRules.visitKeys( id -> {System.out.println( String.valueOf( id ) ); return false;} );
        System.out.println( "PROPERTIES" );
        printSorted( propertyRecords );
//        indexRules.visitKeys( id -> {System.out.println( String.valueOf( id ) ); return false;} );
    }

    private static void printSorted( PrimitiveLongSet indexRules )
    {
        long[] ids = PrimitiveLongCollections.asArray( indexRules.iterator() );
        Arrays.sort( ids );
        for ( long l : ids )
        {
            System.out.println( l );
        }
    }
}

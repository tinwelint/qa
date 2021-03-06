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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.neo4j.helpers.Format.duration;

public class SortTestTimes
{
    private static class TestTime
    {
        private final String name;
        private final long time;

        TestTime( String name, long time )
        {
            this.name = name;
            this.time = time;
        }
    }

    private static final Comparator<TestTime> SORTER = new Comparator<TestTime>()
    {
        @Override
        public int compare( TestTime o1, TestTime o2 )
        {
            return -Long.compare( o1.time, o2.time );
        }
    };

    private static final String magicName = " - in ";
    private static final String magicTime = "Time elapsed: ";

    public static void main( String[] args ) throws Exception
    {
        PrintStream out = System.out;
        try ( BufferedReader reader = new BufferedReader( new FileReader( new File( args[0] ) ) ) )
        {
            List<TestTime> tests = new ArrayList<>();
            String line;
            while ( (line = reader.readLine()) != null )
            {
                try
                {
                    if ( line.contains( "Tests run:" ) && line.contains( magicName ) && line.contains( magicTime ) )
                    {
                        tests.add( new TestTime( parseTestName( line ), parseTime( line ) ) );
                    }
                }
                catch ( Exception e )
                {
                    System.out.println( line );
                    throw new RuntimeException( e );
                }
            }

            Collections.sort( tests, SORTER );

            long total = 0;
            for ( TestTime test : tests )
            {
                System.out.println( test.name );
                total += test.time;
            }
            System.out.println( "Total time: " + duration( total ) );
        }
    }

    private static String parseTestName( String line )
    {
        return line.substring( line.indexOf( magicTime ) + magicTime.length() );
    }

    private static long parseTime( String line )
    {
        String timeString = line.substring( line.indexOf( magicTime ) + magicTime.length(), line.indexOf( magicName ) );
        String[] units = timeString.split( " " );
        long total = 0;
        for ( int i = 0; i < units.length; i++ )
        {
            String value = units[i++];
            String unit = units[i];
            if ( unit.equals( "sec" ) )
            {
                total += (Double.parseDouble( value ) * 1_000);
            }
            else
            {
                throw new IllegalArgumentException( unit );
            }
        }
        return total;
    }
}

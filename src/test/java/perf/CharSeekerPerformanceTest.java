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

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.csv.reader.SectionedCharBuffer;
import org.neo4j.csv.reader.ThreadAheadReadable;

import static org.junit.Assert.assertTrue;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.duration;

public class CharSeekerPerformanceTest
{
    private static final char DELIMITER = ',';
    private static final char QUOTE = '"';

    @Test
    public void shouldMeasurePerformance() throws Exception
    {
        // GIVEN
        ValueType[] types = new ValueType[] {ValueType.LONG, ValueType.STRING, ValueType.QUOTED_STRING};
        CharReadable reader = endlessStreamOfRandomStuff( types );
        CharSeeker seeker = charSeeker( reader, Configuration.DEFAULT.bufferSize(), true, QUOTE );
        Mark mark = new Mark();
        Extractors extractors = new Extractors( ',' );

        // WHEN
        final long startTime = currentTimeMillis();
        final AtomicInteger lines = new AtomicInteger();
        new Thread()
        {
            @Override
            public void run()
            {
                while ( true )
                {
                    try
                    {
                        Thread.sleep( 5_000 );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }

                    long duration = currentTimeMillis()-startTime;
                    System.out.println( lines + " in " + duration( duration ) + " ==> " +
                            ((double)lines.get() / duration) + " lines/ms" );
                }
            }
        }.start();

        while ( true )
        {
            for ( ValueType type : types )
            {
                assertTrue( seeker.seek( mark, DELIMITER ) );
            }
            assertTrue( mark.isEndOfLine() );
            lines.incrementAndGet();
        }
    }

    @Test
    public void shouldMeasureThreadAheadPerformanceProblems() throws Exception
    {
        // GIVEN
        final CharReadable reader = ThreadAheadReadable.threadAhead(
                endlessStreamOfRandomStuff( ValueType.LONG, ValueType.STRING, ValueType.QUOTED_STRING ),
                Configuration.DEFAULT.bufferSize() );

        // WHEN
        SectionedCharBuffer buffer = new SectionedCharBuffer( Configuration.DEFAULT.bufferSize() );
        final AtomicLong read = new AtomicLong();
        final long startTime = currentTimeMillis();
        Thread stats = new Thread()
        {
            @Override
            public void run()
            {
                while ( true )
                {
                    try
                    {
                        Thread.sleep( 2000 );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }

                    System.out.println( bytes( read.get() / (currentTimeMillis()-startTime) ) + "/s " + reader );
                }
            }
        };
        stats.start();
        while ( true )
        {
            buffer = reader.read( buffer, buffer.pivot() );
            read.addAndGet( buffer.available() );
        }
    }

    private CharReadable endlessStreamOfRandomStuff( final ValueType... types )
    {
        return new CharReadable.Adapter()
        {
            // Contains only one line
            private final CharBuffer temp = CharBuffer.wrap( new char[1000] );
            private final Random random = new Random();
            {
                temp.limit( 0 );
            }
            private long totalRead;

            @Override
            public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
            {
//                int cursor = 0;
//                while ( cursor < length )
//                {
//                    if ( !temp.hasRemaining() )
//                    {
//                        fillOneRowIntoTemp();
//                    }
//                    int charsToWrite = min( length-cursor, temp.remaining() );
//                    temp.get( buffer, cursor, charsToWrite );
//                    cursor += charsToWrite;
//                }
//                // We can assume here that we always fill the buffer up
//                totalRead += length;
//                return length;
                throw new UnsupportedOperationException();
            }

            private void fillOneRowIntoTemp()
            {
                temp.clear();
                int i = 0;
                for ( ValueType type : types )
                {
                    if ( i++ > 0 )
                    {
                        temp.put( DELIMITER );
                    }
                    type.appendTo( random, temp );
                }
                temp.put( '\n' );
                temp.flip();
            }

            @Override
            public long position()
            {
                return totalRead;
            }

            @Override
            public String sourceDescription()
            {
                return "source";
            }
        };
    }

    private enum ValueType
    {
        LONG
        {
            @Override
            void appendTo( Random random, CharBuffer buffer )
            {
                long value = random.nextInt( 1_000_000_000 );
                buffer.put( String.valueOf( value ) ); // TODO Wasteful
            }

            @Override
            Extractor<?> extractor( Extractors extractors )
            {
                return extractors.long_();
            }
        },
        STRING
        {
            @Override
            void appendTo( Random random, CharBuffer buffer )
            {
                int length = random.nextInt( 100 ) + 5;
                for ( int i = 0; i < length; i++ )
                {
                    buffer.append( CHARS[random.nextInt( CHARS.length )] );
                }
            }

            @Override
            Extractor<?> extractor( Extractors extractors )
            {
                return extractors.string();
            }
        },
        QUOTED_STRING
        {
            @Override
            void appendTo( Random random, CharBuffer buffer )
            {
                int length = random.nextInt( 100 ) + 5;
                int innerQuoteLength = random.nextInt( length / 2 );
                int innerQuotePosition = random.nextInt( length - innerQuoteLength );

                buffer.append( QUOTE );
                for ( int i = 0; i < length; i++ )
                {
                    if ( innerQuoteLength > 0 )
                    {
                        if ( i == innerQuotePosition || i == innerQuotePosition+innerQuoteLength )
                        {
                            buffer.put( QUOTE ).put( QUOTE );
                        }
                    }
                    buffer.append( CHARS[random.nextInt( CHARS.length )] );
                }
                buffer.append( QUOTE );
            }

            @Override
            Extractor<?> extractor( Extractors extractors )
            {
                return extractors.string();
            }
        };

        private static final char[] CHARS = new char[] {
            'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x',
            'y','z','å','ä','ö','1','2','3','4','5','6','7','8','9','0',' ',
        };

        abstract void appendTo( Random random, CharBuffer buffer );

        abstract Extractor<?> extractor( Extractors extractors );
    }
}

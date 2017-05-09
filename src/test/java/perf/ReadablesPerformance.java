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

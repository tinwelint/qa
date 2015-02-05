

import java.io.File;
import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.CharSeekers;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.csv.reader.Readables;

public class CheckLdbcExternalPropertiesOrder
{
    private static int[] delim = new int[] {'|'};
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
        final CharSeeker seeker = CharSeekers.charSeeker( Readables.file( file ), '"' );
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
        int[] delim = new int[0];
        while ( seeker.seek( mark, delim ) )
        {
            if ( mark.isEndOfLine() )
            {
                break;
            }
        }
    }
}

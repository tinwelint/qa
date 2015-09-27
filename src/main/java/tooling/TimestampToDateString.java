package tooling;

import static java.lang.Long.parseLong;
import static java.lang.System.out;

import static org.neo4j.helpers.Format.date;

public class TimestampToDateString
{
    public static void main( String[] args )
    {
        out.println( date( parseLong( args[0] ) ) );
    }
}

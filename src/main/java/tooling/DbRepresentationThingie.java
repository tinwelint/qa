package tooling;

import java.io.File;

import org.neo4j.test.DbRepresentation;

public class DbRepresentationThingie
{
    public static void main( String[] args )
    {
        System.out.println( DbRepresentation.of( new File( args[0] ) ) );
    }
}

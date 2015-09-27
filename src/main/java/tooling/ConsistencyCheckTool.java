package tooling;

import java.io.IOException;

public class ConsistencyCheckTool
{
    public static void main( String[] args ) throws IOException
    {
        org.neo4j.consistency.ConsistencyCheckTool.main( args );
    }
}

package tooling;

import org.neo4j.shell.StartClient;

public class StartShell
{
    public static void main( String[] args )
    {
        StartClient.main( new String[] {"-path", args[0]} );
    }
}

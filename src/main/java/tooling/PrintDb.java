package tooling;

import versiondiff.VersionDifferences;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class PrintDb
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = VersionDifferences.newDb( args[0] );
        try ( Transaction tx = db.beginTx() )
        {
            int nodes = 0;
            for ( Node node : db.getAllNodes() )
            {
                print( node );
                for ( Relationship relationship : node.getRelationships() )
                {
                    print( relationship );
                }

                if ( nodes++ >= 100 )
                {
                    System.out.println( "... breaking ..." );
                    break;
                }
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void print( Relationship relationship )
    {
        System.out.println( relationship );
        printProperties( relationship );
    }

    private static void print( Node node )
    {
        System.out.println( node );
        printProperties( node );
    }

    private static void printProperties( PropertyContainer entity )
    {
        for ( Map.Entry<String,Object> property : entity.getAllProperties().entrySet() )
        {
            System.out.println( "  " + property );
        }
    }
}

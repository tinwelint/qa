package tooling;

import versiondiff.VersionDifferences;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.impl.store.RelationshipStore;

public class LookForRelationshipWithMinusNodes
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( new File( args[0] ) );
        try
        {
            RelationshipStore store = VersionDifferences.neoStores( db ).getRelationshipStore();
            VersionDifferences.scanAllRecords( store, relationship ->
            {
                if ( relationship.getFirstNode() == -1 || relationship.getSecondNode() == -1 )
                {
                    System.out.println( "Yup " + relationship );
                }
                return false;
            } );
        }
        finally
        {
            db.shutdown();
        }
    }
}

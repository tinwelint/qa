package tooling;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class UpgradeDb
{
    public static void main( String[] args )
    {
        new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( args[0] )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" )
                .newGraphDatabase()
                .shutdown();
    }
}

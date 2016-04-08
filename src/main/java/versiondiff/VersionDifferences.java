package versiondiff;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.tooling.GlobalGraphOperations;

public class VersionDifferences
{
    // 3.0

    // 2.3
    public static NeoStores neoStores( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( NeoStores.class );
    }

    public static <RECORD extends AbstractBaseRecord> void scanAllRecords( RecordStore<RECORD> store,
            Visitor<RECORD,RuntimeException> visitor )
    {
        long lowId = store.getNumberOfReservedLowIds();
        long highId = store.getHighId();
        for ( long id = lowId; id < highId; id++ )
        {
            try
            {
                visitor.visit( store.getRecord( id ) );
            }
            catch ( InvalidRecordException e )
            {   // OK
            }
        }
    }

    public static Iterable<Relationship> getAllRelationships( GraphDatabaseService db )
    {
        return GlobalGraphOperations.at( db ).getAllRelationships();
    }

    public static Label label( String name )
    {
        return DynamicLabel.label( name );
    }

    public static IndexConfiguration nonUniqueIndexConfiguration()
    {
        return new IndexConfiguration( false );
    }

    // 2.2
}

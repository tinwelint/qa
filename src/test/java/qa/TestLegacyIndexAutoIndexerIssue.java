package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

public class TestLegacyIndexAutoIndexerIssue
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected GraphDatabaseBuilder newBuilder( GraphDatabaseFactory factory )
        {
            return super.newBuilder( factory )
                    .setConfig( GraphDatabaseSettings.relationship_auto_indexing, "true" )
                    .setConfig( GraphDatabaseSettings.relationship_keys_indexable, "keyo" );
        }
    };

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            relationship = db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
        }

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            relationship.setProperty( "keyo", "value" );
            tx.success();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            relationship.setProperty( "keyo", "value" );
            tx.success();
        }
    }
}

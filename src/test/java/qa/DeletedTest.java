package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

public class DeletedTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void shouldDoStuffOnDeletedRelationship() throws Exception
    {
        // GIVEN
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            rel = db.createNode().createRelationshipTo( db.createNode(), DynamicRelationshipType.withName( "BLA" ) );
            rel.setProperty( "a", "b" );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            rel.delete();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
//            System.out.println( rel.getAllProperties() );
            System.out.println( rel.getProperty( "a" ) );
            tx.success();
        }
    }
}

package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

public class SimpleOperationsTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldSetNodeProperty() throws Exception
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "1", "2" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node.getProperty( "1" );
            tx.success();
        }
    }
}

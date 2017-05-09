package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.Iterators.single;

public class CreateNativeSchemaNumberIndex
{
    private static final String KEY = "key";
    private static final Label LABEL = Label.label( "Label" );

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        Node[] nodes = new Node[10];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                Node node = nodes[i] = db.createNode( LABEL );
                node.setProperty( KEY, (long)i );
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
        Thread.sleep( SECONDS.toMillis( 5 ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( KEY, 1000L );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                assertEquals( nodes[i], single( db.findNodes( LABEL, KEY, (long) i ) ) );
            }
            tx.success();
        }
    }
}

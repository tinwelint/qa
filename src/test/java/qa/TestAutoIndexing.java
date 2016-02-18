package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAutoIndexing
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() )
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.node_auto_indexing, Settings.TRUE );
            builder.setConfig( GraphDatabaseSettings.node_keys_indexable, "a,b,d" );
        }
    };

    @Test
    public void shouldAutoIndexNoes() throws Exception
    {
        // GIVEN
        Node a, b, c, d;
        try ( Transaction tx = db.beginTx() )
        {
            a = createIndexedNode( "a" );
            b = createIndexedNode( "b" );
            c = createIndexedNode( "c" );
            d = createIndexedNode( "d" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // WHEN
            assertEquals( a, db.index().getNodeAutoIndexer().getAutoIndex().get( "a", 1010 ).getSingle() );
            assertEquals( b, db.index().getNodeAutoIndexer().getAutoIndex().get( "b", 1010 ).getSingle() );
            assertNull( db.index().getNodeAutoIndexer().getAutoIndex().get( "c", 1010 ).getSingle() );
            assertEquals( d, db.index().getNodeAutoIndexer().getAutoIndex().get( "d", 1010 ).getSingle() );
            tx.success();
        }
    }

    private Node createIndexedNode( String key )
    {
        Node node = db.createNode();
        node.setProperty( key, 1010 );
        return node;
    }
}

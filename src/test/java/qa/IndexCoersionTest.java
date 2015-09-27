package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

public class IndexCoersionTest
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldGetHitsOnlyForCorrectValue() throws Exception
    {
        // GIVEN
        long value1 = 437859347589345784L;
        long value2 = 437859347589345785L;
        Label label = DynamicLabel.label( "Label" );
        String key = "key";
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label ).setProperty( key, value1 );
            db.createNode( label ).setProperty( key, value2 );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            System.out.println( db.getGraphDatabaseService().findNode( label, key, value1 ) );
            System.out.println( db.getGraphDatabaseService().findNode( label, key, value2 ) );
            tx.success();
        }
    }
}

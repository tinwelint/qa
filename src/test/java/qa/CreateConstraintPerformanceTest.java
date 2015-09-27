package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

import static java.lang.System.currentTimeMillis;

public class CreateConstraintPerformanceTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();
    private final Label label = DynamicLabel.label( "Labulina" );
    private final String key = "key";

    @Test
    public void shouldCreateTheConstraint() throws Exception
    {
        // GIVEN
        Workers<Runnable> workers = new Workers<>( "YEAH" );
        for ( int i = 0; i < 100; i++ )
        {
            final int thread = i;
            workers.start( new Runnable()
            {
                @Override
                public void run()
                {
                    for ( int i = 0; i < 100; i++ )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            for ( int j = 0; j < 1_000; j++ )
                            {
                                db.createNode( label ).setProperty( key, "value-" + thread + "-" + i + "-" + j );
                            }
                            tx.success();
                        }
                    }
                }
            } );
        }
        workers.awaitAndThrowOnError();

        // WHEN
        System.out.println( "Creating constraint" );
        long time = System.currentTimeMillis();
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }
        time = currentTimeMillis()-time;
        System.out.println( time );
    }
}

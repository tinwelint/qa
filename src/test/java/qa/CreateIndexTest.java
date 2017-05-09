package qa;

import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CreateIndexTest
{
//    @Rule
//    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() )
//    {
//        @Override
//        protected void configure( org.neo4j.graphdb.factory.GraphDatabaseBuilder builder )
//        {
//            builder.setConfig( GraphDatabaseFacadeFactory.Configuration.lock_manager, "community" );
//        }
//    };

    @Test
    public void shouldCreateIndex() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( "yoyo" ) );
        if (
                true )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( Label.label( "kjdfk" ) ).assertPropertyIsUnique( "yo" ).create();
                tx.success();
            }
        }
        db.shutdown();
    }
}

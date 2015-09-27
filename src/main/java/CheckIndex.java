import java.io.File;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class CheckIndex
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( args[0] ) );

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition def : db.schema().getIndexes() )
            {
                System.out.println( def );
            }

            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                System.out.println( constraint );
            }

            for ( Node node : loop( db.findNodes( DynamicLabel.label( "State" ), "GLN", "8884162342237" ) ) )
            {
                System.out.println( node );
            }
            tx.success();
        }
        db.shutdown();
    }
}

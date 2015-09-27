package qa;

import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.MyRelTypes;

public class BiggerIndexIdSpaceFormatChangeTest
{
    private final String storeDir = "target/test-data/bla";

    @Test
    public void shouldCreateTheDbWith_2_2_3_andThenCrash() throws Exception
    {
        // GIVEN
        FileUtils.deleteRecursively( new File( storeDir ) );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );

        // WHEN
        doTransaction( db, "key1", "key2" );

        // THEN
        System.exit( 1 );
    }

    @Test
    public void shouldBeAbleToRecoverAndAddMoreWith_2_2_4() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );

        // WHEN
        doTransaction( db, "key3", "key4" );

        // THEN
        db.shutdown();
    }

    private void doTransaction( GraphDatabaseService db, String nodeKey, String relKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( "baneling" );
            RelationshipIndex relIndex = db.index().forRelationships( "zealot" );
            nodeIndex.add( db.createNode(), nodeKey, "value" );
            relIndex.add( db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST ), relKey, "value" );
            tx.success();
        }
    }
}

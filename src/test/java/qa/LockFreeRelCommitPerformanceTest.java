package qa;

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;
import qa.perf.Operations;
import qa.perf.Performance;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class LockFreeRelCommitPerformanceTest
{
    @Test
    public void shouldPerfTest() throws Exception
    {
        GraphDatabaseTarget target = new GraphDatabaseTarget();
        Operation<GraphDatabaseTarget> commit = new Operation<GraphDatabaseTarget>()
        {
            private final RelationshipType[] types = new RelationshipType[] {
                    DynamicRelationshipType.withName( "TYPE1" ),
                    DynamicRelationshipType.withName( "TYPE2" ),
                    DynamicRelationshipType.withName( "TYPE3" ),
            };

            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    Node node = on.db.createNode();
                    for ( int i = 0; i < 10; i++ )
                    {
                        node.createRelationshipTo( on.db.createNode(), types[i%types.length] );
                    }
                    tx.success();
                }
            }
        };
        Performance.measure( target, null, Operations.single( commit ), 1, 60 );
    }
}

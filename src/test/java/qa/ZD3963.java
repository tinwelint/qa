package qa;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.Race;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.RepeatRule.Repeat;

/**
 * Race: all involved threads running:
 *
 * - MERGE (n0:MyTestLabel {`id`: {param0}}) SET n0 = {param1}
 *
 * SCENARIO 1, WHEN NODE EXISTS FROM THE GET GO
 *
 * - T1 asks about node for the given "id" and so acquires SHARED INDEX_ENTRY lock for the given label/key combination
 * - T2 asks about node for the given "id" and so acquires SHARED INDEX_ENTRY lock for the given label/key combination
 * - T1 sets label or property and so acquires EXCLUSIVE NODE lock in LockingStatementOperations
 * - T1 continues down the kernel cake and wants to acquire EXCLUSIVE INDEX_ENTRY lock since it's "changing" this node,
 *      blocks since T2 has the SHARED INDEX_ENTRY lock
 * - T2 sets label or property and so wants to acquire EXCLUSIVE NODE lock in LockingStatementOperations --> DEADLOCK
 *
 * SCENARIO 2, WHEN NODE DOESN'T EXIST FROM THE GET GO
 *
 * Requires at least 3 threads, since the node doesn't exist and so they will all block in the initial
 * ConstraintEnforcingEntityOperations#nodeGetFromUniqueIndexSeek where one will win and go all the way creating
 * the node and what not, the others will later downgrade their INDEX_ENTRY to SHARED and run into the same problem
 * as scenario 1.
 */
public class ZD3963
{
    private static final Label LABEL = Label.label( "MyTestLabel" );
    private static final String MERGE = "MERGE (n0:" + LABEL.name() + " {`id`: {param0}}) SET n0 = {param1}";
    private static final String ID = "05adbde2-2740-44f2-bb88-3ff049676f66";
    private static Map<String,Object> VALUES;
    static
    {
        VALUES = new HashMap<>();
        VALUES.put( "id", ID );
        VALUES.put( "value", "whatever" );
        VALUES.put( "key", "settings" );
    }

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final RepeatRule repeater = new RepeatRule();

    @Repeat( times = 1000 )
    @Test
    public void shouldNotDeadlock() throws Throwable
    {
        // GIVEN
        createConstraint();
        deleteAllNodes();
        // Comment this if you want to try out scenario 2
        runMergeQuery();

        // WHEN
        Race race = new Race();
        int numberOfThreads = 3;
        race.addContestants( numberOfThreads, () -> runMergeQuery() );
        race.go();
    }

    private void deleteAllNodes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                node.delete();
            }
            tx.success();
        }
    }

    private void createConstraint()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CREATE CONSTRAINT ON (p:MyTestLabel) ASSERT p.id IS UNIQUE" );
            tx.success();
        }
    }

    private void runMergeQuery()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "param0", ID );
            params.put( "param1", VALUES );
            db.execute( MERGE, params );
            tx.success();
        }
    }
}

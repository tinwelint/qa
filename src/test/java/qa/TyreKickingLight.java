package qa;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;

public class TyreKickingLight
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void shouldFailSlaveTxImmediatelyAfter() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();
        System.out.println( "shutdown " + cluster.getMaster() );
        cluster.shutdown( cluster.getMaster() );

        // WHEN
        try
        {
            for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
            {
                System.out.println( db );
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode();
                    tx.success();
                }
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

        // THEN
        // cool
    }
}

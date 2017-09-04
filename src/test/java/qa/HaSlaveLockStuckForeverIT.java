/**
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package qa;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertFalse;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * 2.2
 * - Forseti: NO
 * - Community: NO
 * 2.3
 * - Forseti: NO
 * - Community: NO
 * 3.0 - 3.3
 * - Forseti: YES
 * - Community: YES
 */
public class HaSlaveLockStuckForeverIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( HaSettings.tx_push_factor, "2" )
            .withSharedSetting( KernelTransactions.tx_termination_aware_locks, "true" )
            .withSharedSetting( GraphDatabaseFacadeFactory.Configuration.lock_manager, "community" );
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldTest() throws Throwable
    {
        try
        {
            // given
            ManagedCluster cluster = clusterRule.startCluster();
            final HighlyAvailableGraphDatabase master = cluster.getMaster();
            final long nodeId;
            try ( Transaction tx = master.beginTx() )
            {
                nodeId = master.createNode().getId();
                tx.success();
            }
            final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            Transaction zombieTx = slave.beginTx();
            slave.getNodeById( nodeId ).setProperty( "key", "yo" );
            System.out.println( "Slave got lock on " + nodeId );
            // and just leave it there

            // when
            final AtomicBoolean masterLockedTheNode = new AtomicBoolean();
            Future<Void> slaveTx = t2.execute( new WorkerCommand<Void,Void>()
            {
                @Override
                public Void doWork( Void state ) throws Exception
                {
                    master.beginTx();
                    System.out.println( "Master attempting lock on " + nodeId );
                    try
                    {
                        master.getNodeById( nodeId ).setProperty( "yo", "yo" );
                        System.out.println( "Master got lock on " + nodeId );
                    }
                    catch ( Exception e )
                    {
                        System.out.println( "Failed getting it" );
                    }
                    // ... will block
                    masterLockedTheNode.set( true );
                    return null;
                }
            } );
            Thread.sleep( 2_000 );
            assertFalse( masterLockedTheNode.get() );

            // now do a master switch
            System.out.println( "Failing master " + master );
            RepairKit masterRepair = cluster.fail( master );
            cluster.await( ClusterManager.masterAvailable( master ) );
            masterRepair.repair();
            cluster.await( ClusterManager.allSeesAllAsAvailable() );
            HighlyAvailableGraphDatabase newMaster = cluster.getMaster();
            System.out.println( "Noticed new master " + newMaster );
//            assertNotSame( master, newMaster );

            System.out.println( "Seeing if the master tx somehow gets fixed" );
            try
            {
                slaveTx.get( 10, SECONDS );
                System.out.println( "Yes OK" );
            }
            catch ( TimeoutException e )
            {
                System.out.println( "No did not" );
                for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
                {
                    try ( Transaction tx = master.beginTx() )
                    {
                        System.out.println( "Attempting to acquire this lock on " + db );
                        master.getNodeById( nodeId ).setProperty( "sds", 1 );
                        System.out.println( "worked!" );
                        tx.success();
                    }
                }
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
    }
}

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HaIndexEntryDeadlockStressTest
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void shouldStressIt() throws Exception
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        final Label label = DynamicLabel.label( "Label" );
        final String key = "key";
        try ( Transaction tx = master.beginTx() )
        {
            master.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }
        try ( Transaction tx = master.beginTx() )
        {
            master.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
        cluster.sync();

        final IndexDescriptor index;
        try ( Transaction tx = master.beginTx() )
        {
            ThreadToStatementContextBridge bridge = master.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );
            try ( Statement statement = ktx.acquireStatement() )
            {
                ReadOperations ops = statement.readOperations();
                index = ops.indexGetForLabelAndPropertyKey( ops.labelGetForName( label.name() ),
                        ops.propertyKeyGetForName( key ) );
            }
            tx.success();
        }

        Workers<Runnable> workers = new Workers<>( getClass().getSimpleName() );
        final AtomicInteger deadlocks = new AtomicInteger();
        final AtomicInteger txCount = new AtomicInteger();
        final AtomicInteger violations = new AtomicInteger();
        final long end = currentTimeMillis() + SECONDS.toMillis( 30 );
        for ( final HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            for ( int i = 0; i < 3/*on each db*/; i++ )
            {
                workers.start( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            int myDeadlocks = 0, myTxCount = 0, myViolations = 0;
                            ThreadLocalRandom random = ThreadLocalRandom.current();
                            ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency(
                                    ThreadToStatementContextBridge.class );
                            while ( currentTimeMillis() < end )
                            {
                                try ( Transaction tx = db.beginTx() )
                                {
                                    KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );
                                    for ( int j = 0; j < 10; j++ )
                                    {
                                        Object propertyValue = random.nextInt( 100 );
                                        if ( random.nextFloat() > 0.1 )
                                        {
                                            Node node = db.createNode( label );
                                            node.setProperty( key, propertyValue );
                                            myTxCount++;
                                        }
//                                    else
//                                    {
//                                        try ( Statement statement = ktx.acquireStatement() )
//                                        {
//                                            statement.readOperations().nodeGetUniqueFromIndexLookup( index, propertyValue );
//                                        }
//                                        catch ( KernelException e )
//                                        {
//                                            throw new RuntimeException( e );
//                                        }
//                                    }
                                    }
                                    tx.success();
                                }
                                catch ( DeadlockDetectedException e )
                                {
                                    myDeadlocks++;
                                }
                                catch ( ConstraintViolationException e )
                                {
                                    myViolations++;
                                }
                            }
                            deadlocks.addAndGet( myDeadlocks );
                            txCount.addAndGet( myTxCount );
                            violations.addAndGet( myViolations );
                        }
                        catch ( RuntimeException e )
                        {
//                            Locks.LOCK_GRAPH.print( System.out );
//                            e.printStackTrace();
                        }
                    }
                } );
            }
        }

        boolean success = false;
        try
        {
            // THEN
            workers.awaitAndThrowOnError( Exception.class );
            success = true;
        }
        finally
        {
//            if ( !success )
//            {
//                Locks.LOCK_GRAPH.print( System.out );
//            }
        }

        System.out.println(
                "deadlocks:" + deadlocks + ", " +
                "txCount:" + txCount + ", " +
                "violations:" + violations );
    }

    private Iterable<HighlyAvailableGraphDatabase> oneSlave( ManagedCluster cluster )
    {
        Collection<HighlyAvailableGraphDatabase> members = new ArrayList<>();
        members.add( cluster.getAnySlave() );
        return members;
    }

    private Iterable<HighlyAvailableGraphDatabase> slaveAndMaster( ManagedCluster cluster )
    {
        Collection<HighlyAvailableGraphDatabase> members = new ArrayList<>();
        members.add( cluster.getMaster() );
        members.add( cluster.getAnySlave() );
        return members;
    }

    private Iterable<HighlyAvailableGraphDatabase> slaves( ManagedCluster cluster )
    {
        Collection<HighlyAvailableGraphDatabase> slaves = new ArrayList<>();
        while ( true )
        {
            try
            {
                slaves.add( cluster.getAnySlave( slaves.toArray( new HighlyAvailableGraphDatabase[slaves.size()] ) ) );
            }
            catch ( IllegalStateException e )
            {
                break;
            }
        }
        return slaves;
    }
}

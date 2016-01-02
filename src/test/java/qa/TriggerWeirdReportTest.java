/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertTrue;

import static java.lang.String.format;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TriggerWeirdReportTest
{
    @Rule
    public final RepeatRule repeatRule = new RepeatRule();
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Test
    @RepeatRule.Repeat( times = 1000 )
    public void aaaa() throws Throwable
    {
        File seedDir = new File( "K:\\issues\\local\\db-for-new-cc" );

        File root = testDirectory.graphDbDir();
        FileUtils.deleteRecursively( root );

        ClusterManager clusterManager = new ClusterManager.Builder( root ).withSeedDir( seedDir )
                .withProvider( ClusterManager.clusterOfSize( 2 ) ).withDbFactory( new TestHighlyAvailableGraphDatabaseFactory() )
                .withSharedConfig( stringMap(
                        GraphDatabaseSettings.store_internal_log_level.name(), "DEBUG",
                        ClusterSettings.default_timeout.name(), "1s",
                        HaSettings.tx_push_factor.name(), "0",
                        GraphDatabaseSettings.pagecache_memory.name(), "8m" ) ).build();
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

        cluster.await( ClusterManager.allSeesAllAsAvailable() );

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        File masterStoreDirectory = master.getStoreDirectory();
        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        File slaveStoreDirectory = slave.getStoreDirectory();
        final CountDownLatch latch = new CountDownLatch( 2 );
        Thread label = new Thread()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = slave.beginTx() )
                {
                    latch.countDown();
                    latch.await();
                    Node node = slave.getNodeById( 10062 );
                    node.removeLabel( DynamicLabel.label( "Label-62" ) );
                    tx.success();
                }
                catch ( Throwable e )
                {
                    e.printStackTrace();
                }
            }
        };
        Thread rel = new Thread()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = slave.beginTx() )
                {
                    latch.countDown();
                    latch.await();
                    Node node = slave.getNodeById( 10062 );
                    node.createRelationshipTo( slave.createNode(), DynamicRelationshipType.withName( "TREE" ) );
                    tx.success();
                }
                catch ( Throwable e )
                {
                    e.printStackTrace();
                }
            }
        };
        rel.start();
        label.start();
        rel.join();
        label.join();
        clusterManager.shutdown();
        assertConsistentStore( masterStoreDirectory );
        System.out.println( format( "%n*************************************************************%n" ) );
    }

    private void assertConsistentStore( File dir ) throws Exception
    {
        final Config configuration = new Config( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m" ),
                GraphDatabaseSettings.class, ConsistencyCheckSettings.class );

        // Experimental
        final ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck(
                dir, configuration, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false );

        // Legacy
//        final org.neo4j.legacy.consistency.ConsistencyCheckService.Result result = new org.neo4j.legacy.consistency.ConsistencyCheckService().runFullConsistencyCheck(
//              dir, configuration, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), new DefaultFileSystemAbstraction() );

        assertTrue( result.isSuccessful() );
    }
}

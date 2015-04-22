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

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;

public class StartLocalHaCluster
{
    public static void main( String[] args ) throws Exception
    {
        final int basePort = 5001;
        final File rootPath = new File( "cluster" );
        final GraphDatabaseAPI[] dbs = new GraphDatabaseAPI[3];
        System.out.println( "Starting cluster..." );
        final CountDownLatch latch = new CountDownLatch( dbs.length );
        for ( int i = 0; i < dbs.length; i++ )
        {
            final int finalI = i;
            new Thread()
            {
                @Override
                public void run()
                {
                    GraphDatabaseAPI db = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory()
                            .newHighlyAvailableDatabaseBuilder( new File( rootPath, "" + finalI ).getAbsolutePath() )
                            .setConfig( ClusterSettings.server_id, "" + finalI )
                            .setConfig( ClusterSettings.cluster_server, ":" + (basePort+finalI) )
                            .setConfig( ClusterSettings.initial_hosts,
                                    ":" + (basePort) + ",:" + (basePort+1) + ",:" + (basePort+2) )
                            .newGraphDatabase();
                    dbs[finalI] = db;
                    latch.countDown();
                }
            }.start();
            System.out.println( "Started db " + finalI );
        }
        
        latch.await();
        System.out.println( "Waiting for them to form cluster" );
        waitForAllSeesAll( dbs );
        
        System.out.println( "Started, ENTER to quit" );
        System.in.read();
        for ( GraphDatabaseService item : dbs )
        {
            item.shutdown();
        }
    }

    private static void waitForAllSeesAll( GraphDatabaseAPI[] dbs ) throws InterruptedException
    {
        long end = System.currentTimeMillis() + MINUTES.toMillis( 1 );
        while ( currentTimeMillis() < end )
        {
            for ( GraphDatabaseAPI db : dbs )
            {
                ClusterMembers members = db.getDependencyResolver().resolveDependency( ClusterMembers.class );

                int memberCount = 0;
                for ( ClusterMember clusterMember : members.getMembers() )
                {
                    if ( clusterMember.getHARole().equals( "UNKNOWN" ) )
                    {
                        break;
                    }
                    memberCount++;
                }
                if ( memberCount == dbs.length )
                {
                    return;
                }
            }
            Thread.sleep( 10 );
        }
        throw new RuntimeException( "Didn't come up" );
    }
}

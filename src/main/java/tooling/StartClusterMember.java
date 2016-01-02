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
package tooling;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

public class StartClusterMember
{
//    public static final String ip = "172.16.13.47";

    private static final String root = "cluster";

    public static void main( String[] args ) throws IOException, Exception
    {
        int offset = Integer.parseInt( args[0] );
        String dir = new File( root, "" + offset ).getAbsolutePath();
//        FileUtils.deleteRecursively( new File( dir ) );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, "10G size" )
                .setConfig( ClusterSettings.server_id, "" + (1+offset) )
                .setConfig( ClusterSettings.cluster_server, server( 5001+offset ) )
                .setConfig( ClusterSettings.initial_hosts, server( 5001 ) + "," + server( 5002 ) + "," + server( 5003 ) )
                .setConfig( OnlineBackupSettings.online_backup_server, server( 6362+offset ) )
                .setConfig( HaSettings.tx_push_strategy, HaSettings.TxPushStrategy.fixed.name() )
                .newGraphDatabase();

        LastCommittedTxMonitor txMonitor = new LastCommittedTxMonitor(
                db.getDependencyResolver().resolveDependency( TransactionIdStore.class ) );

        System.out.println( "up " + offset );
        BufferedReader input = new BufferedReader( new InputStreamReader( System.in ) );
        while ( true )
        {
            String line = input.readLine();
            System.out.println( "'" + line + "'" );
            if ( line == null || line.equals( "exit" ) || line.equals( "quit" ) )
            {
                break;
            }

            if ( line.equals( "tx" ) )
            {
                doTx( db );
            }
            else if ( line.equals( "members" ) )
            {
                listMembers( db );
            }
        }

        txMonitor.cancel();
        db.shutdown();
    }

    private static void listMembers( GraphDatabaseAPI db )
    {
        System.out.println( db.getDependencyResolver().resolveDependency( ClusterMembers.class ) );
    }

    public static String server( int port )
    {
        return ":" + port;
    }

    private static void doTx( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }
}

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

import java.io.File;
import java.util.concurrent.Future;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.shell.ShellSettings;

public class StartClusterMaster
{
    public static final String ip = "172.16.13.47";
    private static final String dir = "C:\\Users\\Matilas\\Desktop\\master";

    public static void main( String[] args ) throws Exception
    {
        FileUtils.deleteRecursively( new File( dir ) );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, "10G size" )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( ClusterSettings.cluster_server, server( 5001 ) )
                .setConfig( ClusterSettings.initial_hosts, server( 5001 ) + "," + server( 5002 ) + "," + server( 5003 ) )
                .setConfig( OnlineBackupSettings.online_backup_server, server( 6362 ) )
                .setConfig( ShellSettings.remote_shell_enabled, "true" )
                .setConfig( HaSettings.tx_push_factor, "0" )
                .newGraphDatabase();

        LastCommittedTxMonitor txMonitor = new LastCommittedTxMonitor(
                db.getDependencyResolver().resolveDependency( TransactionIdStore.class ) );

        System.out.println( "up, ENTER for starting load" );
        System.in.read();
        Future<Object> load = new SillyLoad( db, 30, 1000 );
        System.out.println( "loading, ENTER for shutdown" );
        System.in.skip( System.in.available() );
        System.in.read();
        load.cancel( false );
        txMonitor.cancel();
        load.get();
        db.shutdown();
    }

    public static String server( int port )
    {
        return ip + ":" + port;
    }
}

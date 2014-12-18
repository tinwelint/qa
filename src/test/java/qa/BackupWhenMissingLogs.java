/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

import org.junit.Test;

import org.neo4j.backup.BackupTool;
import org.neo4j.backup.OnlineBackup;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.test.TargetDirectory;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class BackupWhenMissingLogs
{
    private static final TargetDirectory directory = TargetDirectory.forTest( BackupWhenMissingLogs.class );

    @Test
    public void shouldCreateASillyDb() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "silly" );
        createTransaction( db );
        db.shutdown();
    }

    @Test
    public void doBackup() throws Exception
    {
        OnlineBackup.from( "localhost" ).backup( directory.cleanDirectory( "backup" ).getAbsolutePath() );
    }

    @Test
    public void shouldBackupUsingBackupTool() throws Exception
    {
        BackupTool.main( new String[] {
                "-from=single://localhost",
                "-to=backup-w-tool",
        } );
    }

    public static void main( String[] args ) throws InterruptedException
    {
        // Start a db that is capable of serving backups
        final GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                directory.makeGraphDbDir().getAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, "true" )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, "3 files" )
                .newGraphDatabase();

        new Thread()
        {
            {   // Constructor, sort of
                start();
            }

            @Override
            public void run()
            {
                for ( String line : readInputLines( "exit", "quit" ) )
                {
                    switch ( line )
                    {
                    case "batch":
                        createTransactionBatchAndRotate( db );
                        System.out.println( "Created a batch" );
                        break;
                    }
                }
            }
        }.join();

        db.shutdown();
    }

    protected static void createTransactionBatchAndRotate( GraphDatabaseAPI db )
    {
        for ( int i = 0; i < 10; i++ )
        {
            createTransaction( db );
        }
        db.getDependencyResolver().resolveDependency( DataSourceManager.class ).rotateLogicalLogs();
    }

    private static void createTransaction( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    protected static Iterable<String> readInputLines( String... exitCommands )
    {
        final Set<String> exits = asSet( exitCommands );
        final BufferedReader reader = new BufferedReader( new InputStreamReader( System.in ) );
        return loop( new PrefetchingIterator<String>()
        {
            @Override
            protected String fetchNextOrNull()
            {
                try
                {
                    String line = reader.readLine();
                    return exits.contains( line ) ? null : line;
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
    }
}

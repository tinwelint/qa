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

import java.io.File;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import static org.neo4j.helpers.Format.bytes;

public class LogPruningSettingTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() )
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.keep_logical_logs,
//                    "10M size"
                    "5 files"
                    );
            builder.setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1M" );
        }
    };

    @Test
    public void shouldRespectPruneSetting() throws Exception
    {
        new Thread()
        {
            @Override
            public void run()
            {
                File storeDir = db.getStoreDirFile();
                while ( true )
                {
                    parkNanos( SECONDS.toNanos( 5 ) );
                    listTransactionFiles( storeDir );
                }
            }

            private void listTransactionFiles( File storeDir )
            {
                System.out.println( "=== LIST ===" );
                long totalSize = 0;
                for ( File file : storeDir.listFiles() )
                {
                    if ( file.getName().startsWith( PhysicalLogFile.DEFAULT_NAME ) )
                    {
                        totalSize += file.length();
                        System.out.println( file.getName() + ": " + bytes( file.length() ) );
                    }
                }
                System.out.println( "TOTAL " + bytes( totalSize ) );
            }
        }.start();

        while ( true )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < 10; i++ )
                {
                    db.createNode().setProperty( "key", i );
                }
                tx.success();
            }
        }
    }
}

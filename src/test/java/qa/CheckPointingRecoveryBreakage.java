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

import org.junit.Test;
import versiondiff.VersionDifferences;

import java.io.File;

import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.ProcessUtil;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CheckPointingRecoveryBreakage
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );

    @Test
    public void shouldBreakRecovery() throws Exception
    {
        String dir = "target/checkpoint-breakage";
        for ( int i = 0; i < 100; i++ )
        {
            FileUtils.deleteRecursively( new File( dir ) );
            ProcessUtil.executeSubProcess( getClass(), 10, MINUTES, dir );

            VersionDifferences.newDb( dir ).shutdown();
            ConsistencyCheckTool.main( new String[] {dir} );
        }
    }

    public static void main( String[] args ) throws Exception
    {
        long checkPointIntervalMs = 500;
        final GraphDatabaseService db = VersionDifferences.newDbBuilder( args[0] )
                .setConfig( GraphDatabaseSettings.check_point_interval_time, String.valueOf( checkPointIntervalMs ) + "ms" )
                .newGraphDatabase();

        for ( int i = 0; i < 100; i++ )
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    try
                    {
                        while ( true )
                        {
                            try ( Transaction tx = db.beginTx() )
                            {
                                Node node = db.createNode();
                                node.createRelationshipTo( node, TYPE );
                                tx.success();
                            }
                        }
                    }
                    catch ( Throwable e )
                    {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

        final long end = currentTimeMillis() + checkPointIntervalMs * 5;
        System.out.println( "end " + end + ", current " + currentTimeMillis() );
        while ( currentTimeMillis() < end )
        {
            Thread.sleep( 200 );
            System.out.println( "end " + end + ", current " + currentTimeMillis() );
        }
        System.out.println( "exit" );
        System.exit( 0 );
    }
}

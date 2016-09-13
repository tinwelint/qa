/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.Race;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;
import org.neo4j.test.TargetDirectory;

public class ShutdownWhileRunningTransactions
{
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final RepeatRule repeater = new RepeatRule();

    @Repeat( times = 100 )
    @Test
    public void shouldSeeWhatHappens() throws Exception
    {
        System.out.println( "----" );
        // GIVEN
        File storeDir = directory.directory();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );

        // WHEN
        Race race = new Race();
        for ( int i = 0; i < 20; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    while ( true )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            db.createNode();
                            tx.success();
                        }
                        catch ( Throwable t )
                        {
                            // OK
                            return;
                        }
                    }
                }
            } );
        }

        for ( int i = 0; i < 2; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    for ( int j = 0; j < 2; j++ )
                    {
                        try
                        {
                            Thread.sleep( (j == 0 ? 2000 : 0) + random.nextInt( 1000 ) );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }
                        db.shutdown();
                    }
                }
            } );
        }

        // WHEN
        try
        {
            race.go();
        }
        catch ( Throwable e )
        {
            // OK
        }

        GraphDatabaseService db2 = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
        db2.shutdown();
    }
}

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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.ThreadTestUtils;

import static org.junit.Assert.assertEquals;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SimplifiedNeoStoreDataSourceStopTest
{
    @Test
    public void shouldWaitForCommittingInFlightTransactionsAWhile() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        int txs = 10;
        ExecutorService executor = Executors.newFixedThreadPool( txs + 1 );

        // WHEN
        AtomicBoolean end = new AtomicBoolean();
        for ( int i = 0; i < txs; i++ )
        {
            executor.submit( tx( db, i ) );
        }
        executor.submit( () ->
        {
            while ( !end.get() )
            {
                try
                {
                    Thread.sleep( 1_000 );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                ThreadTestUtils.dumpAllStackTraces();
            }
        } );

        // THEN
        db.shutdown();
        end.set( true );
        List<Runnable> left = executor.shutdownNow();
        assertEquals( 0, left.size() );
    }

    private Runnable tx( GraphDatabaseService db, int i )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                try
                {
                    Thread.sleep( SECONDS.toMillis( i ) );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                tx.success();
            }
        };
    }
}

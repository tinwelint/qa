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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GH6036
{
    private GraphDatabaseService database;

    @Before
    public void setUp()
    {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( "lock_manager", "community" )
                .newGraphDatabase();
        try ( Transaction tx = database.beginTx() )
        {
            database.createNode( DynamicLabel.label( "Test" ) );
            tx.success();
        }
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void verifyConcurrentInsertsAndFetchesFromTheLinkedList() throws InterruptedException
    {
        ExecutorService createExecutor = Executors.newFixedThreadPool( 50 );
        int noRequests = 1000;
        final AtomicBoolean success = new AtomicBoolean( true );
        for ( int i = 0; i < noRequests; i++ )
        {
            createExecutor.execute( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        increment();
                    }
                    catch ( Exception e )
                    {
                        success.set( false );
                        e.printStackTrace();
                    }
                }
            } );
            createExecutor.execute( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
//                        System.out.println( "counter = " + get() );
                        get();
                    }
                    catch ( Exception e )
                    {
                        success.set( false );
                        e.printStackTrace();
                    }
                }
            } );
        }
        long start = System.currentTimeMillis();
        createExecutor.shutdown();
        createExecutor.awaitTermination( 120, TimeUnit.SECONDS );
        System.out.println( "Took " + (System.currentTimeMillis() - start) + " ms" );
        assertTrue( "At least one operation failed", success.get() );
        try ( Transaction tx = database.beginTx() )
        {
            assertEquals( noRequests, database.getNodeById( 0 ).getProperty( "counter" ) );
            tx.success();
        }
    }

    private void increment()
    {
        try ( Transaction tx = database.beginTx() )
        {
            Node node = database.getNodeById( 0 );
            tx.acquireWriteLock( node );
            node.setProperty( "counter", (int) node.getProperty( "counter", 0 ) + 1 );
            tx.success();
        }
    }

    private int get()
    {
        int result = -1;
        try ( Transaction tx = database.beginTx() )
        {
            Node node = database.getNodeById( 0 );
            tx.acquireReadLock( node );
            result = (int) node.getProperty( "counter", 0 );
            tx.success();
        }
        return result;
    }
}

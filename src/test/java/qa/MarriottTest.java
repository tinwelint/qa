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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;

// On 2.2.8
public class MarriottTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

//    @Test
//    public void shouldClearCache() throws Exception
//    {
//        // GIVEN
//        GraphDatabaseService db = this.db;
//        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( Caches.class ).clear();
//
//        // WHEN
//        try ( Transaction tx = db.beginTx() )
//        {
//            db.createNode();
//            tx.success();
//        }
//
//        // THEN
//    }

    @Test
    public void shouldSeeWhetherOrNotInTxAlready() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = this.db;
        System.out.println( ((GraphDatabaseAPI)db).getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class ).hasTransaction() );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            System.out.println( ((GraphDatabaseAPI)db).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class ).hasTransaction() );

            tx.success();
        }
    }

    @Test
    public void shouldStressTheIssue() throws Exception
    {
        // GIVEN
        datasetOf( 100_000 );

        // WHEN
        ExecutorService executor = Executors.newCachedThreadPool();
        for ( int i = 0; i < 1_000; i++ )
        {
            final Node node;
            try ( Transaction tx = db.beginTx() )
            {
                // do tx which changes properties
                node = db.getNodeById( i );
                changeNode( node );
                tx.success();
            }
            // start another thread which are supposed to see the changes
            executor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        assertEquals( 100, node.getProperty( "changed-0" ) );
                        assertEquals( 100, node.getProperty( "key-5" ) );
                        tx.success();
                    }
                }
            } ).get();
        }
    }

    private void changeNode( Node node )
    {
        node.setProperty( "key-5", 100 );
        node.setProperty( "changed-0", 100 );
    }

    private void datasetOf( int nodes )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes; i++ )
            {
                createNodeWithInitialProperties();
            }
            tx.success();
        }
    }

    private Node createNodeWithInitialProperties()
    {
        Node node = db.createNode();
        for ( int i = 0; i < 10; i++ )
        {
            node.setProperty( "key-" + i, i );
        }
        return node;
    }
}

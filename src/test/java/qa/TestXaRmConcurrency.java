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

import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class TestXaRmConcurrency
{
    @Test
    public void should() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbr.getGraphDatabaseService();
        Node rootNode = createSomeData( db );
        ExecutorService executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );

        // WHEN
        Runnable reader = readOperation( db, rootNode );
        System.out.println( "Ready?" );
        new BufferedReader( new InputStreamReader( System.in ) ).readLine();
        long time = currentTimeMillis();
        for ( int i = 0; i < 10_000_000; i++ )
        {
            executor.submit( reader );
        }
        executor.shutdown();
        executor.awaitTermination( 100, SECONDS );
        time = currentTimeMillis()-time;

        // THEN
        System.out.println( time );
    }

    private Runnable readOperation( final GraphDatabaseService db, final Node rootNode )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    count( rootNode.getRelationships() );
                    tx.success();
                }
            }
        };
    }

    private Node createSomeData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node rootNode = db.createNode();
            createChildren( db, rootNode, 1, 3 );
            tx.success();
            return rootNode;
        }
    }

    private void createChildren( GraphDatabaseService db, Node parent, int depth, int maxDepth )
    {
        if ( depth >= maxDepth )
        {
            return;
        }

        for ( int i = 0; i < maxDepth; i++ )
        {
            Node child = db.createNode();
            parent.createRelationshipTo( child, MyRelTypes.TEST );
            createChildren( db, child, depth+1, maxDepth );
        }
    }

    public final @Rule ImpermanentDatabaseRule dbr = new ImpermanentDatabaseRule();
}

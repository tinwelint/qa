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

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.DoubleLatch;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class TestUpgradeCypherShaiThingie
{
    @Test
    public void shouldGetSome() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(
                "C:\\Users\\Mattias\\Work\\issues\\Shai-2.1.2-upgrade\\graph.db" );
        try
        {
            final ExecutionEngine cypher = new ExecutionEngine( db );
            final String query = "match (pr:Property {name: \"BLACK\"}) " + "match (pr)--(it) return pr,it";
            final CountDownLatch startSignal = new CountDownLatch( 1 );

            // WHEN
            Thread[] threads = new Thread[8];
            for ( int i = 0; i < threads.length; i++ )
            {
                threads[i] = new Thread()
                {
                    @Override
                    public void run()
                    {
                        DoubleLatch.awaitLatch( startSignal );
                        while ( true )
                        {
                            ExecutionResult result = cypher.execute( query );
                            try ( ResourceIterator<?> iterator = result.iterator() )
                            {
                                int count = count( iterator );
                                System.out.println( "=> " + count );
                            }
                        }
                    }
                };
            }
            for ( Thread item : threads )
            {
                item.start();
            }
            startSignal.countDown();
            for ( Thread item : threads )
            {
                item.join();
            }
        }
        finally
        {
            db.shutdown();
        }
    }
}

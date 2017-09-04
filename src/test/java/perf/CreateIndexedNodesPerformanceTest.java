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
package perf;

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;
import qa.perf.Performance;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import static qa.perf.Operations.single;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.Iterators.count;

public class CreateIndexedNodesPerformanceTest
{
    private static final Label LABEL = Label.label( "Name" );
    private static final String KEY = "Name";

    @Test
    public void shouldMeasurePerformance() throws Exception
    {
        long start = currentTimeMillis();
        GraphDatabaseTarget target = new GraphDatabaseTarget(
//                GraphDatabaseSettings.label_index.name(), "lucene"
                );

        Operation<GraphDatabaseTarget> createIndex = on ->
        {
            try ( Transaction tx = on.db.beginTx() )
            {
                on.db.schema().indexFor( LABEL ).on( KEY ).create();
                tx.success();
            }
            try
            {
                Thread.sleep( SECONDS.toMillis( 1 ) );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        };

        Operation<GraphDatabaseTarget> createIndexedNode = on ->
        {
            try ( Transaction tx = on.db.beginTx() )
            {
                Node node = on.db.createNode( LABEL );
                node.setProperty( KEY, currentTimeMillis() );
                tx.success();
            }
        };

        Operation<GraphDatabaseTarget> findNodes = on ->
        {
            try ( Transaction tx = on.db.beginTx() )
            {
                try ( ResourceIterator<Node> nodes = on.db.findNodes( LABEL, KEY,
                        ThreadLocalRandom.current().nextLong( start, currentTimeMillis() ) ) )
                {
                    count( nodes );
                }
                tx.success();
            }
        };

        Performance.measure( target, createIndex, single( createIndexedNode ), 20, MINUTES.toSeconds( 1 ) );
    }
}

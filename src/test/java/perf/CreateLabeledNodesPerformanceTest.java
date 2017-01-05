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

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.first;

public class CreateLabeledNodesPerformanceTest
{
    private enum Labels implements Label
    {
        One,
        Two,
        Three,
        Four;
    }
    private static final Label[] LABELS = Labels.values();

    @Test
    public void shouldMeasurePerformance() throws Exception
    {
        GraphDatabaseTarget target = new GraphDatabaseTarget();

        Operation<GraphDatabaseTarget> createManyEmptyNodes = new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    try ( Transaction tx = on.db.beginTx() )
                    {
                        for ( int j = 0; j < 100_000; j++ )
                        {
                            on.db.createNode();
                        }
                        tx.success();
                    }
                }
            }
        };

        Operation<GraphDatabaseTarget> createLabeledNode = new Operation<GraphDatabaseTarget>()
        {
            private final Label me = label( "Me" );

            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    on.db.createNode( me ); // me.... me, me, meee
                    tx.success();
                }
                try ( Transaction tx = on.db.beginTx();
                        ResourceIterator<Node> nodes = on.db.findNodes( me ) )
                {
                    first( nodes );
                    tx.success();
                }
            }
        };

        Operation<GraphDatabaseTarget> changeLabels = new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                try ( Transaction tx = on.db.beginTx() )
                {
                    for ( int i = 0; i < 10/*tx size*/; i++ )
                    {
                        long nodeId = random.nextLong( 10_000_000 );
                        Label label = LABELS[random.nextInt( LABELS.length )];
                        if ( random.nextFloat() < 0.9 )
                        {
                            on.db.getNodeById( nodeId ).addLabel( label );
                        }
                        else
                        {
                            on.db.getNodeById( nodeId ).removeLabel( label );
                        }
                    }
                    tx.success();
                }
            }
        };

        Performance.measure( target, createManyEmptyNodes, single( changeLabels ), 100, MINUTES.toSeconds( 1 ) );
    }
}

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

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;
import qa.perf.Performance;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import static qa.perf.Operations.single;

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.first;

public class CreateLabeledNodesPerformanceTest
{
    @Test
    public void shouldMeasurePerformance() throws Exception
    {
        GraphDatabaseTarget target = new GraphDatabaseTarget();
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
                try ( Transaction tx = on.db.beginTx() )
                {
                    first( GlobalGraphOperations.at( on.db ).getAllNodesWithLabel( me ) );
                    tx.success();
                }
            }
        };
        Performance.measure( target, null, single( createLabeledNode ), 4, MINUTES.toSeconds( 2 ) );
    }
}

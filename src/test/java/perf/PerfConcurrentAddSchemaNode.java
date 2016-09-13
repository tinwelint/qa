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
import qa.perf.Operations;
import qa.perf.Performance;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static java.util.concurrent.TimeUnit.SECONDS;

public class PerfConcurrentAddSchemaNode
{
    private static final Label label = DynamicLabel.label( "Yo" );
    private static final String key = "key";

    @Test
    public void shouldPerform() throws Exception
    {
        // GIVEN
        GraphDatabaseTarget target = new GraphDatabaseTarget();
        Operation<GraphDatabaseTarget> createIndex = new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    on.db.schema().indexFor( label ).on( key ).create();
                    tx.success();
                }
                try ( Transaction tx = on.db.beginTx() )
                {
                    on.db.schema().awaitIndexesOnline( 10, SECONDS );
                    tx.success();
                }
            }
        };
        Operation<GraphDatabaseTarget> createNode = new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                try ( Transaction tx = on.db.beginTx() )
                {
                    int value = random.nextInt();
                    for ( int i = 0; i < 10; i++ )
                    {
                        on.db.createNode( label ).setProperty( key, value + i );
                    }
                    tx.success();
                }
            }
        };
        Performance.measure( target, createIndex, Operations.single( createNode ), 100, 60 );
    }
}

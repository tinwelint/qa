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
import qa.perf.Operations;
import qa.perf.Performance;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class LockFreeRelCommitPerformanceTest
{
    @Test
    public void shouldPerfTest() throws Exception
    {
        GraphDatabaseTarget target = new GraphDatabaseTarget();
        Operation<GraphDatabaseTarget> commit = new Operation<GraphDatabaseTarget>()
        {
            private final RelationshipType[] types = new RelationshipType[] {
                    DynamicRelationshipType.withName( "TYPE1" ),
                    DynamicRelationshipType.withName( "TYPE2" ),
                    DynamicRelationshipType.withName( "TYPE3" ),
            };

            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    Node node = on.db.createNode();
                    for ( int i = 0; i < 10; i++ )
                    {
                        node.createRelationshipTo( on.db.createNode(), types[i%types.length] );
                    }
                    tx.success();
                }
            }
        };
        Performance.measure( target, null, Operations.single( commit ), 1, 60 );
    }
}

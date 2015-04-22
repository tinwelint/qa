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

import org.neo4j.graphdb.Transaction;

import static qa.perf.Operations.single;

public class PerformanceJavaAPITest
{
    @Test
    public void shouldPerformanceTestGetNodeById() throws Exception
    {
        Performance.measure( new GraphDatabaseTarget(), new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    on.db.createNode();
                    tx.success();
                }
            }
        }, single( new Operation<GraphDatabaseTarget>()
        {
//            private final RelationshipType type = DynamicRelationshipType.withName( "PAJAS" );

            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    on.db.getNodeById( 0 );
//                    Node from = on.db.createNode();
//                    Node to = on.db.createNode();
//                    from.createRelationshipTo( to, type );
                    tx.success();
                }
            }
        } ), Runtime.getRuntime().availableProcessors(), 180 );
    }
}

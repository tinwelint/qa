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

public class IdGeneratorCommitContentionTest
{
    @Test
    public void shouldRunIt() throws Exception
    {
        Operation<GraphDatabaseTarget> operation = Operations.inTx( on ->
        {
            for ( int i = 0; i < 100; i++ )
            {
                on.db.createNode();
            }
        } );

        Performance.measure( new GraphDatabaseTarget() )
                .withThreads( 300 )
                .withDuration( 30 )
                .withOperations( operation )
                .please();
    }
}

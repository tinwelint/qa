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

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class LazyIndexUpdatesVerificationTest
{
    private final String directory = "C:\\Users\\Matilas\\Desktop\\69DB862DCE05E3E7390CD75B7AE85681";

    @Test
    public void shouldHmm() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory );
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                node.getProperty( "key" );
            }
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }
}

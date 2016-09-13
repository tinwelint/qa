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
package qa;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static java.lang.System.currentTimeMillis;

public class CreateSomeSmallTransactions
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "smalltxs" );
        try
        {
            for ( int i = 0; i < 100; i++ )
            {
                createSmallTransaction( db );
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void createSmallTransaction( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            for ( int i = 0; i < currentTimeMillis() % 3 + 1; i++ )
            {
                node.createRelationshipTo( db.createNode(), DynamicRelationshipType.withName( "TYPE" + i ) );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}

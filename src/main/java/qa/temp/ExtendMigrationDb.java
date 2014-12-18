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
package qa.temp;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.DynamicLabel.label;

public class ExtendMigrationDb
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
                {
                    System.out.println( node );
                    for ( Relationship rel : node.getRelationships() )
                    {
                        System.out.println( "  " + rel );
                    }
                }
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                node.setProperty( "name", "Johnny Labels" );
                for ( int i = 0; i < 30; i++ )
                {
                    node.addLabel( label( "AlterEgo" + i ) );
                }
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }
}

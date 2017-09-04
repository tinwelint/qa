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

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class RelationshipsHasPropertyTest
{
    public static void main( String[] args )
    {
        RelationshipType FRIEND = DynamicRelationshipType.withName( "FRIEND" );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );
        try ( Transaction tx = db.beginTx() )
        {
            long i = 0;
            for ( Relationship rel : GlobalGraphOperations.at( db ).getAllRelationships() )
            {
                if ( rel.isType( FRIEND ) && !rel.hasProperty( "since" ) )
                {
                    System.out.println( rel );
                }
                i++;
                if ( i % 1_000_000 == 0 )
                {
                    System.out.println( i );
                }
            }
            tx.success();
        }
        db.shutdown();
    }
}

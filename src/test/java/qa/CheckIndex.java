package qa;
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
import java.io.File;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class CheckIndex
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( args[0] ) );

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition def : db.schema().getIndexes() )
            {
                System.out.println( def );
            }

            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                System.out.println( constraint );
            }

            for ( Node node : loop( db.findNodes( DynamicLabel.label( "State" ), "GLN", "8884162342237" ) ) )
            {
                System.out.println( node );
            }
            tx.success();
        }
        db.shutdown();
    }
}

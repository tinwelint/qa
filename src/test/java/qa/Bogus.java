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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.fail;

public class Bogus
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        RelationshipType type = DynamicRelationshipType.withName( "YO" );
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                node.createRelationshipTo( db.createNode(), type );
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node.addLabel( DynamicLabel.label( "Hi" ) );
            tx.success();
        }

        // WHEN
        // THEN
        fail( "Test not fully implemented yet" );
    }
}

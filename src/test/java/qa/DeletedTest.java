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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

public class DeletedTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void shouldDoStuffOnDeletedRelationship() throws Exception
    {
        // GIVEN
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            rel = db.createNode().createRelationshipTo( db.createNode(), DynamicRelationshipType.withName( "BLA" ) );
            rel.setProperty( "a", "b" );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            rel.delete();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
//            System.out.println( rel.getAllProperties() );
            System.out.println( rel.getProperty( "a" ) );
            tx.success();
        }
    }
}

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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

public class TestLegacyIndexAutoIndexerIssue
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected GraphDatabaseBuilder newBuilder( GraphDatabaseFactory factory )
        {
            return super.newBuilder( factory )
                    .setConfig( GraphDatabaseSettings.relationship_auto_indexing, "true" )
                    .setConfig( GraphDatabaseSettings.relationship_keys_indexable, "keyo" );
        }
    };

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            relationship = db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
        }

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            relationship.setProperty( "keyo", "value" );
            tx.success();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            relationship.setProperty( "keyo", "value" );
            tx.success();
        }
    }
}

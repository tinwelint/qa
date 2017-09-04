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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAutoIndexing
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() )
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.node_auto_indexing, Settings.TRUE );
            builder.setConfig( GraphDatabaseSettings.node_keys_indexable, "a,b,d" );
        }
    };

    @Test
    public void shouldAutoIndexNoes() throws Exception
    {
        // GIVEN
        Node a, b, c, d;
        try ( Transaction tx = db.beginTx() )
        {
            a = createIndexedNode( "a" );
            b = createIndexedNode( "b" );
            c = createIndexedNode( "c" );
            d = createIndexedNode( "d" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // WHEN
            assertEquals( a, db.index().getNodeAutoIndexer().getAutoIndex().get( "a", 1010 ).getSingle() );
            assertEquals( b, db.index().getNodeAutoIndexer().getAutoIndex().get( "b", 1010 ).getSingle() );
            assertNull( db.index().getNodeAutoIndexer().getAutoIndex().get( "c", 1010 ).getSingle() );
            assertEquals( d, db.index().getNodeAutoIndexer().getAutoIndex().get( "d", 1010 ).getSingle() );
            tx.success();
        }
    }

    private Node createIndexedNode( String key )
    {
        Node node = db.createNode();
        node.setProperty( key, 1010 );
        return node;
    }
}

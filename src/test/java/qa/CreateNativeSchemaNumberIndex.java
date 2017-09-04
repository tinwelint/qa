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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.Iterators.single;

public class CreateNativeSchemaNumberIndex
{
    private static final String KEY = "key";
    private static final Label LABEL = Label.label( "Label" );

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        Node[] nodes = new Node[10];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                Node node = nodes[i] = db.createNode( LABEL );
                node.setProperty( KEY, (long)i );
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
        Thread.sleep( SECONDS.toMillis( 5 ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( KEY, 1000L );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                assertEquals( nodes[i], single( db.findNodes( LABEL, KEY, (long) i ) ) );
            }
            tx.success();
        }
    }
}

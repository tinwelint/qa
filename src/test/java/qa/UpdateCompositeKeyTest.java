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

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

public class UpdateCompositeKeyTest
{
    public static final Label LABEL = Label.label( "Label" );
    public static final String KEY = "1";

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void shouldTest() throws Exception
    {
        System.out.println( db.getStoreDir().getAbsolutePath() );
        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            // given
            index = db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10000, TimeUnit.SECONDS );
            tx.success();
        }
        catch ( IllegalStateException e )
        {
            String failure;
            try ( Transaction tx = db.beginTx() )
            {
                failure = db.schema().getIndexFailure( index );
                tx.success();
            }
            System.out.println( failure );
            throw e;
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( KEY, "a" );
            db.createNode( LABEL ).setProperty( KEY, 12345 );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node foundNode1 = db.findNode( LABEL, KEY, "a" );
            Node foundNode2 = db.findNode( LABEL, KEY, 12345 );
            System.out.println( foundNode1 );
            System.out.println( foundNode2 );
            tx.success();
        }
    }
}

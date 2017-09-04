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

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.concurrent.TimeUnit.MINUTES;

public class SimpleOperationsTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldSetNodeProperty() throws Exception
    {
        Label label = DynamicLabel.label( "Label" );
        String key = "key";
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( key ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, MINUTES );
            tx.success();
        }

        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( label );
            node.setProperty( key, "2" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node.getProperty( key );
            tx.success();
        }
    }
}

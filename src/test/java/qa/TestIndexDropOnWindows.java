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

import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.FileUtils;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class TestIndexDropOnWindows
{
    @Test
    public void shouldDropIndexes() throws Exception
    {
        // GIVEN
        String path = "target/db";
        FileUtils.deleteRecursively( new File( path ) );
        GraphDatabaseService  db = new GraphDatabaseFactory().newEmbeddedDatabase( path );

        for ( int i = 0; i < 3; i++ )
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    while ( true )
                    {
                        Object[] array = new Object[10000];
                        for ( int j = 0; j < array.length; j++ )
                        {
                            array[j] = new long[10000];
                        }
                    }
                }
            }.start();
        }

        // WHEN
        while ( true )
        {
            createPopulateDropIndex( db );
        }

        // THEN
    }

    private static final Label label = label( "Label" );
    private static final String key = "dfjkdjf";

    private void createPopulateDropIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( key ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 30, SECONDS );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 1000; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( key, "something " + i );
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = single( db.schema().getIndexes( label ) );
            index.drop();
            tx.success();
        }
    }
}

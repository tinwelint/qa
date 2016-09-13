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
package perf;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class TestBigQueryLegacyIndexMemoryIssue
{
    private final String storeDir = "target/test-data/" + getClass().getSimpleName();
    private final String key = "key";

    @Test
    public void shouldCreateBigIndex() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        Index<Node> index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.index().forNodes( "anton" );
            tx.success();
        }

        for ( int i = 0; i < 5; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int j = 0; j < 1_000_000; j++ )
                {
                    Node node = db.createNode();
                    index.add( node, key, "a" + i );
                }
                tx.success();
            }
            System.out.println( i );
        }
        db.shutdown();
    }

    @Test
    public void shouldNotRequireMuchMemoryAtAll() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        Index<Node> index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.index().forNodes( "anton" );
            tx.success();
        }

        long time = currentTimeMillis();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                Node node = db.getNodeById( i );
                index.remove( node );
                index.add( node, key, "aaa" + i );
            }

            // WHEN
            try ( IndexHits<Node> hits = index.query( key, "a*" ) )
            {
                int count = count( (Iterator<Node>) hits );
                System.out.println( "hits:" + count );
            }
            tx.success();
        }
        time = currentTimeMillis()-time;
        System.out.println( duration( time ) );

        // THEN
        db.shutdown();
    }
}

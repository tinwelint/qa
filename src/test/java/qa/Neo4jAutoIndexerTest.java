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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class Neo4jAutoIndexerTest
{
    private static GraphDatabaseService graphDb;

    @BeforeClass
    public static void beforeClass()
    {
        graphDb =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( "target/" + System.currentTimeMillis() )
//                        .setConfig( GraphDatabaseSettings.relationship_keys_indexable, "Type" )
//                        .setConfig( GraphDatabaseSettings.relationship_auto_indexing, "true" )
                        .newGraphDatabase();
    }

    @AfterClass
    public static void afterClass()
    {
        graphDb.shutdown();
    }

    @Test
    public void testIssue()
    {
        long startId;
        long endId;
        Transaction tx = graphDb.beginTx();
        try
        {
            Node start = graphDb.createNode();
            Node end = graphDb.createNode();
            startId = start.getId();
            endId = end.getId();
            Relationship rel = start.createRelationshipTo( end, RelTypes.TEST );
            rel.setProperty( "Type", RelTypes.TEST.name() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = graphDb.beginTx();
        try
        {
            ReadableRelationshipIndex autoRelationshipIndex =
                    graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
            Node start = graphDb.getNodeById( startId );
            Node end = graphDb.getNodeById( endId );
            IndexHits<Relationship> hits = autoRelationshipIndex.get( "Type", RelTypes.TEST.name(), start, end );
            assertEquals( 1, count( (Iterator<Relationship>)hits ) );
            assertEquals( 1, hits.size() );
            start.getRelationships().forEach( new Consumer<Relationship>()
            {
                @Override
                public void accept( Relationship t )
                {
                    t.delete();
                }
            } );
            autoRelationshipIndex = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
            hits = autoRelationshipIndex.get( "Type", RelTypes.TEST.name(), start, end );
            assertEquals( 0, count( (Iterator<Relationship>)hits ) );
            assertEquals( 0, hits.size() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public enum RelTypes implements RelationshipType
    {
        TEST
    }
}

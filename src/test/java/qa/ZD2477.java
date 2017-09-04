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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.junit.Assert.assertNotNull;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ZD2477
{
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private static final String key1 = "stp_id";
    private static final String key2 = "CustomerCity";
    private static final String key3 = "CustomerAddressLine3";
    private static final String value1 = "Customers:1";
    private static final String value2 = "PARIS";
    private static final String value3 = "Empty";
    private static final String value4 = "Empty";

    /*
     * The attached sample app reproduces a problem in these steps.
        1. Create a batch inserter
        2. Create a node with a single indexed property (_stp_id)
        3. Update that node adding two more indexed properties (CustomerCity and CustomerAddressLine3)
        4. Shutdown the inserter
        5. Open the graph with the embedded graph database service
        6. Note that the the node can be found using an index lookup of CustomerCity
        7. Lookup the node by _stp_id
        8. Update the CustomerAddressLine3 property and reindex it appropriately
        9. Note that you can no longer find the node using an index lookup of CustomerCity
     */
    @Test
    public void shouldReproduce() throws Exception
    {
        // 1. Create a batch inserter
        BatchInserter inserter = BatchInserters.modifier( directory.absolutePath() );
        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);
        BatchInserterIndex batchInserterIndex = indexProvider.nodeIndex( "nodes", stringMap( "type", "exact" ) );

        // 2. Create a node with a single indexed property (_stp_id)
        Map<String,Object> map = map( key1, value1 );
        long nodeId = inserter.createNode( map );
        batchInserterIndex.add( nodeId, map );

        // 3. Update that node adding two more indexed properties (CustomerCity and CustomerAddressLine3)
        Map<String,Object> newMap = map( new HashMap<>( map ), key2, value2, key3, value3 );
        inserter.setNodeProperties( nodeId, newMap );
        batchInserterIndex.add( nodeId, newMap );

        // 4. Shutdown the inserter
        batchInserterIndex.flush();
        indexProvider.shutdown();
        inserter.shutdown();

        // 5. Open the graph with the embedded graph database service
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        Index<Node> nodeIndex;
        try ( Transaction tx = db.beginTx() )
        {
            nodeIndex = db.index().forNodes( "nodes" );
            tx.success();
        }

        // 6. Note that the the node can be found using an index lookup of CustomerCity
        assertNotNull( "Before", queryGraph( db, nodeIndex ) );
        updateGraph( db, nodeIndex );
        assertNotNull( "After", queryGraph( db, nodeIndex ) );
        db.shutdown();
    }

    private Node queryGraph( GraphDatabaseService db, Index<Node> index )
    {
        try ( Transaction tx = db.beginTx() )
        {
            final IndexHits<Node> hits = index.get( key2, value2 );
            final Node single = hits.getSingle();
            tx.success();
            return single;
        }
    }

    private void updateGraph( GraphDatabaseService db, Index<Node> index )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node neoNode = getNeoNodeByProperty( index, key1, value1, value1 );
            neoNode.setProperty( key3, value4 );
            index.remove( neoNode, key3 );
            index.add( neoNode, key3, value4 );
            tx.success();
        }
    }

    private Node getNeoNodeByProperty( Index<Node> index, String attribute, String value, Object indexValue )
    {
        IndexHits<Node> hits = null;
        try
        {
            if ( indexValue == null )
            {
                return null;
            }
            hits = index.get( attribute, indexValue );
            if ( !hits.hasNext() )
            {
                return null;
            }
            Node neoNode = hits.next();
            if ( hits.hasNext() )
            {
                throw new RuntimeException( "More than one node had " + attribute + " of " + value );
            }
            return neoNode;
        }
        finally
        {
            if ( hits != null )
            {
                hits.close();
            }
        }
    }
}

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
package qa;

import org.junit.Rule;
import org.junit.Test;
import versiondiff.VersionDifferences;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

public class LabelsTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

    @Test
    public void shouldHandleThisScenario() throws Exception
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                node.addLabel( VersionDifferences.label( "l" + i ) );
            }
            tx.success();
        }
        NodeStore nodeStore = VersionDifferences.neoStores( db ).getNodeStore();
        long nodeId = 11030;
        nodeStore.setHighId( nodeId );
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int labelId : new int[] {6, 8, 9, 10, 11, 12, 14, 21, 22, 24, 29, 33, 37, 38, 41, 42, 43,
                    47, 48, 51, 52, 56, 60, 61, 64, 67, 69, 70} )
            {
                node.addLabel( VersionDifferences.label( "l" + labelId ) );
            }
            tx.success();
        }

        // WHEN
        System.out.println( ">>>>>>>>>> HERE WE ARE" );
        try ( Transaction tx = db.beginTx() )
        {
            node.removeLabel( VersionDifferences.label( "l" + 56 ) );
            node.addLabel( VersionDifferences.label( "l" + 17 ) );
            node.addLabel( VersionDifferences.label( "l" + 27 ) );
            tx.success();
        }

        NodeRecord record = nodeStore.getRecord( node.getId(), nodeStore.newRecord(), RecordLoad.NORMAL );
        System.out.println( record );
    }
}

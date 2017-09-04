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

import org.junit.Test;
import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.MyRelTypes;

import static versiondiff.VersionDifferences.newDb;

public class BiggerIndexIdSpaceFormatChangeTest
{
    private final String storeDir = "target/test-data/bla";

    @Test
    public void shouldCreateTheDbWith_2_2_3_andThenCrash() throws Exception
    {
        // GIVEN
        FileUtils.deleteRecursively( new File( storeDir ) );
        GraphDatabaseService db = newDb( storeDir );

        // WHEN
        doTransaction( db, "key1", "key2" );

        // THEN
        System.exit( 1 );
    }

    @Test
    public void shouldBeAbleToRecoverAndAddMoreWith_2_2_4() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = newDb( storeDir );

        // WHEN
        doTransaction( db, "key3", "key4" );

        // THEN
        db.shutdown();
    }

    private void doTransaction( GraphDatabaseService db, String nodeKey, String relKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( "baneling" );
            RelationshipIndex relIndex = db.index().forRelationships( "zealot" );
            nodeIndex.add( db.createNode(), nodeKey, "value" );
            relIndex.add( db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST ), relKey, "value" );
            tx.success();
        }
    }
}

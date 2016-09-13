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

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.test.RandomRule;
import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class DPIssue
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldCreateAllTheseRelationshipTypes() throws Exception
    {
        // GIVEN
        FileUtils.deleteRecursively( new File( "dp" ) );
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( "dp" )
                .setConfig( GraphDatabaseSettings.dense_node_threshold, "1" )
                .setConfig( GraphDatabaseSettings.cache_type, "none" )
                .newGraphDatabase();

        // WHEN just creating all these types
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < 34_000; i++ )
            {
                node.createRelationshipTo( db.createNode(), type( i ) );
                if ( i % 100 == 0 )
                {
                    System.out.println( i );
                }
            }
            tx.success();
        }

        // and WHEN creating a node with some low and some high (negative) types
        while ( true )
        {
            System.out.println( "-----" );

            // Create node with random rels and stuff
            Node otherNode;
            List<Relationship> relationships = new ArrayList<>();
            RelationshipType[] types;
            try ( Transaction tx = db.beginTx() )
            {
                otherNode = db.createNode();
                int relCount = random.intBetween( 5, 20 );
                for ( int i = 0; i < relCount; i++ )
                {
                    relationships.add( otherNode.createRelationshipTo( db.createNode(),
                            type( random.intBetween( 31_000, 34_000 ) ) ) );
                }
                types = typesOf( relationships );
                tx.success();
            }

            // Now see if getRelationships always will do the right thing
            for ( int i = 0; i < 100; i++ )
            {
                RelationshipType[] queryTypes = random.selection( types, 1, types.length, false );
                try ( Transaction tx = db.beginTx() )
                {
                    assertEquals( relationshipsOfTypes( relationships, queryTypes ),
                            count( node.getRelationships( queryTypes ) ) );
                    tx.success();
                }
            }
        }
    }

    private int relationshipsOfTypes( List<Relationship> relationships, RelationshipType[] queryTypes )
    {
        int count = 0;
        for ( Relationship relationship : relationships )
        {
            if ( isType( relationship, queryTypes ) )
            {
                count++;
            }
        }
        return count;
    }

    private boolean isType( Relationship relationship, RelationshipType[] queryTypes )
    {
        for ( RelationshipType relationshipType : queryTypes )
        {
            if ( relationship.isType( relationshipType ) )
            {
                return true;
            }
        }
        return false;
    }

    private RelationshipType[] typesOf( Iterable<Relationship> relationships )
    {
        Set<String> types = new HashSet<>();
        for ( Relationship rel : relationships )
        {
            types.add( rel.getType().name() );
        }
        RelationshipType[] result = new RelationshipType[types.size()];
        Iterator<String> foundTypes = types.iterator();
        for ( int i = 0; i < result.length; i++ )
        {
            result[i] = DynamicRelationshipType.withName( foundTypes.next() );
        }
        return result;
    }

    private void printGroupChain( NeoStore stores, long nodeId )
    {
        NodeRecord node = stores.getNodeStore().getRecord( nodeId );
        assert node.isDense();
        long groupId = node.getNextRel();
        while ( !Record.NO_NEXT_RELATIONSHIP.is( groupId ) )
        {
            RelationshipGroupRecord group = stores.getRelationshipGroupStore().getRecord( groupId );
            System.out.println( group );
            groupId = group.getNext();
        }
    }

    private DynamicRelationshipType type( int i )
    {
        return DynamicRelationshipType.withName( "TYPE_" + i );
    }
}

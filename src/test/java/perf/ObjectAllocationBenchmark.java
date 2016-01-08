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
package perf;

import org.junit.Test;
import qa.perf.CountingAllocationSampler;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;
import qa.perf.Performance;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class ObjectAllocationBenchmark
{
    private static final Label LABEL = DynamicLabel.label( "Label" );
    private static final String KEY = "key";
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );

    private void measure( Operation<GraphDatabaseTarget> initial, Operation<GraphDatabaseTarget> op ) throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() )
                .withInitialOperation( initial )
                .withOperations( op )
                .withAllCores()
//                .withWarmup( 100 )
                .withDuration( 5 )
                .withAllocationSampling( new CountingAllocationSampler( 1d ) )
                .withNameFromCallingMethod( 1 )
                .please();
    }

    @Test
    public void createNode() throws Exception
    {
        measure( null, _createNode() );
    }

    @Test
    public void createNodeWithLabel() throws Exception
    {
        measure( null, _createNodeWithLabel() );
    }

    @Test
    public void createNodeWithIntProperty() throws Exception
    {
        measure( null, _createNodeWithIntProperty() );
    }

    @Test
    public void createNodeWithStringProperty() throws Exception
    {
        measure( null, _createNodeWithStringProperty() );
    }

    @Test
    public void createTwoNodesConnectedWithRelationship() throws Exception
    {
        measure( null, _createTwoNodesConnectedWithRelationship() );
    }

    @Test
    public void createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship() throws Exception
    {
        measure( null, _createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship() );
    }

    @Test
    public void readNode() throws Exception
    {
        measure( _createNode(), (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                on.db.getNodeById( 0 );
                tx.success();
            }
        } );
    }

    @Test
    public void findNodeWithLabel() throws Exception
    {
        measure( _createNodeWithLabel(), (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                count( on.db.findNodes( LABEL ) );
                tx.success();
            }
        } );
    }

    @Test
    public void readNodeWithStringProperty() throws Exception
    {
        measure( _createNodeWithStringProperty(), (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                Node node = on.db.getNodeById( 0 );
                node.getProperty( KEY );
                tx.success();
            }
        } );
    }

    @Test
    public void readTwoNodeConnectedWithRelationship() throws Exception
    {
        measure( _createTwoNodesConnectedWithRelationship(), (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                Node start = on.db.getNodeById( 0 );
                on.db.getNodeById( 1 );
                start.getSingleRelationship( TYPE, Direction.OUTGOING );
                tx.success();
            }
        } );
    }

    @Test
    public void readTwoNodesWithLabelAndIntPropertyConnectedWithRelationship() throws Exception
    {
        measure( _createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship(), (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                Node start = on.db.getNodeById( 0 );
                Node end = on.db.getNodeById( 1 );
                start.getSingleRelationship( TYPE, Direction.OUTGOING );
                start.hasLabel( LABEL );
                end.hasLabel( LABEL );
                start.getProperty( KEY );
                end.getProperty( KEY );
                tx.success();
            }
        } );
    }

    private Operation<GraphDatabaseTarget> _createNode()
    {
        return (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                on.db.createNode();
                tx.success();
            }
        };
    }

    private Operation<GraphDatabaseTarget> _createNodeWithLabel()
    {
        return (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                on.db.createNode( LABEL );
                tx.success();
            }
        };
    }

    private Operation<GraphDatabaseTarget> _createNodeWithIntProperty()
    {
        return (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                on.db.createNode().setProperty( KEY, 10 );
                tx.success();
            }
        };
    }

    private Operation<GraphDatabaseTarget> _createNodeWithStringProperty()
    {
        return (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                on.db.createNode().setProperty( KEY, "string" );
                tx.success();
            }
        };
    }

    private Operation<GraphDatabaseTarget> _createTwoNodesConnectedWithRelationship()
    {
        return (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                on.db.createNode().createRelationshipTo( on.db.createNode(), TYPE );
                tx.success();
            }
        };
    }

    private Operation<GraphDatabaseTarget> _createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship()
    {
        return (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                Node start = on.db.createNode( LABEL );
                start.setProperty( KEY, 10 );
                Node end = on.db.createNode( LABEL );
                end.setProperty( KEY, 20 );
                start.createRelationshipTo( end, TYPE );
                tx.success();
            }
        };
    }
}

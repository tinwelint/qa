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
package perf;

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;
import qa.perf.Performance;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import static qa.perf.Operations.inTx;
import static qa.perf.Operations.noop;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class ObjectAllocationBenchmark
{
    private static final Label LABEL = DynamicLabel.label( "Label" );
    private static final String KEY = "key";
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );

    private void measure( Operation<GraphDatabaseTarget> initial, Operation<GraphDatabaseTarget> op ) throws Exception
    {
        Performance.measure( new GraphDatabaseTarget() )
                .withInitialOperation( inTx( initial ) )
                .withOperations( inTx( op ) )
                .withAllCores()
                .withDuration( 20 )
                .withVerboseAllocationSampling()
                .withAllocationSamplingCallStackExclusionByPackage( "org.apache.lucene" )
                .withNameFromCallingMethod( 1 )
                .please();
    }

    private void measureOne( Operation<GraphDatabaseTarget> initial, Operation<GraphDatabaseTarget> op ) throws Exception
    {
        initial = inTx( initial );
        op = inTx( op );
        GraphDatabaseTarget target = new GraphDatabaseTarget();
        target.start();
        try
        {
            initial.perform( target );
            op.perform( target ); // warmup
            op.perform( target );
        }
        finally
        {
            target.stop();
        }
    }

    @Test
    public void createNode() throws Exception
    {
        measure( noop(), _createNode() );
    }

    @Test
    public void createNodeWithLabel() throws Exception
    {
        measure( noop(), _createNodeWithLabel() );
    }

    @Test
    public void createNodeWithIntProperty() throws Exception
    {
        measure( noop(), _createNodeWithIntProperty() );
    }

    @Test
    public void createNodeWithStringProperty() throws Exception
    {
        measure( noop(), _createNodeWithStringProperty() );
    }

    @Test
    public void createIndexedNodeWithIntPropertyAndLabel() throws Exception
    {
        measure( createIndex(), _createNodeWithIntPropertyAndLabel() );
    }

    @Test
    public void createTwoNodesConnectedWithRelationship() throws Exception
    {
        measure( noop(), _createTwoNodesConnectedWithRelationship() );
    }

    @Test
    public void createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship() throws Exception
    {
        measure( noop(), _createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship() );
    }

    @Test
    public void readNode() throws Exception
    {
        measure( _createNode(), (on) -> {
            on.db.getNodeById( 0 );
        } );
    }

    @Test
    public void findNodeWithLabel() throws Exception
    {
        measure( _createNodeWithLabel(), (on) -> {
            count( on.db.findNodes( LABEL ) );
        } );
    }

    @Test
    public void readNodeWithStringProperty() throws Exception
    {
        measure( _createNodeWithStringProperty(), (on) -> {
            Node node = on.db.getNodeById( 0 );
            node.getProperty( KEY );
        } );
    }

    @Test
    public void readTwoNodesConnectedWithRelationship() throws Exception
    {
        measure( _createTwoNodesConnectedWithRelationship(), (on) -> {
            Node start = on.db.getNodeById( 0 );
            on.db.getNodeById( 1 );
            start.getSingleRelationship( TYPE, Direction.OUTGOING );
        } );
    }

    @Test
    public void readTwoNodesWithLabelAndIntPropertyConnectedWithRelationship() throws Exception
    {
        measure( _createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship(), (on) -> {
            Node start = on.db.getNodeById( 0 );
            Node end = on.db.getNodeById( 1 );
            start.getSingleRelationship( TYPE, Direction.OUTGOING );
            start.hasLabel( LABEL );
            end.hasLabel( LABEL );
            start.getProperty( KEY );
            end.getProperty( KEY );
        } );
    }

    private Operation<GraphDatabaseTarget> _createNode()
    {
        return (on) -> {
            on.db.createNode();
        };
    }

    private Operation<GraphDatabaseTarget> _createNodeWithLabel()
    {
        return (on) -> {
            on.db.createNode( LABEL );
        };
    }

    private Operation<GraphDatabaseTarget> _createNodeWithIntProperty()
    {
        return (on) -> {
            on.db.createNode().setProperty( KEY, 10 );
        };
    }

    private Operation<GraphDatabaseTarget> _createNodeWithStringProperty()
    {
        return (on) -> {
            on.db.createNode().setProperty( KEY, "abcdefghijklmnoprqstuvwxyzåäö1234567890!#¤%&/()=?<>;:^¨~HDJFKJDHFKJDHFJDKFHDKJFHDJKFHDJFKHDKFHDFKGHKRGtukrhtljHDLKFJKL" );
        };
    }

    private Operation<GraphDatabaseTarget> _createNodeWithIntPropertyAndLabel()
    {
        return (on) -> {
            on.db.createNode( LABEL ).setProperty( KEY, 10 );
        };
    }

    private Operation<GraphDatabaseTarget> _createTwoNodesConnectedWithRelationship()
    {
        return (on) -> {
            on.db.createNode().createRelationshipTo( on.db.createNode(), TYPE );
        };
    }

    private Operation<GraphDatabaseTarget> _createTwoNodesWithLabelAndIntPropertyConnectedWithRelationship()
    {
        return (on) -> {
            Node start = on.db.createNode( LABEL );
            start.setProperty( KEY, 10 );
            Node end = on.db.createNode( LABEL );
            end.setProperty( KEY, 20 );
            start.createRelationshipTo( end, TYPE );
        };
    }

    private Operation<GraphDatabaseTarget> createIndex()
    {
        return (on) -> {
            on.db.schema().indexFor( LABEL ).on( KEY ).create();
        };
    }
}

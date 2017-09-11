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

import versiondiff.VersionDifferences;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.test.TestGraphDatabaseFactory;

public class RelationshipIndexProblem
{
    GraphDatabaseService graphDatabaseService;
    final Label leftLabel = DynamicLabel.label( "left" );
    final Label rightLabel = DynamicLabel.label( "right" );
    final String nicknameProperty = "nickname";
    final String relationshipIndexProperty = "timestamp";
    RelationshipIndex leftToRightIndex;
    long currentTime = 1000;

    static enum SampleRelationshipType implements RelationshipType
    {
        LEFT_TO_RIGHT
    }

    void createANode( Label label, int nickname )
    {
        Node node = graphDatabaseService.createNode( label );
        node.setProperty( nicknameProperty, nickname );
        System.out.println( "Created a " + label.toString() + " node with nickname " + nickname + "" );
    }

    Node findNodeByLabelAndProperty( Label label, String propertyName, int value )
    {
        final ResourceIterator<Node> results =
                graphDatabaseService.findNodes( label, propertyName, value );
        return results.hasNext() ? results.next() : null;
    }

    Relationship createRelationship( int leftNickname, int rightNickname )
    {
        final Node leftNode = findNodeByLabelAndProperty( leftLabel, nicknameProperty, leftNickname );
        final Node rightNode = findNodeByLabelAndProperty( rightLabel, nicknameProperty, rightNickname );
        Relationship relationship = leftNode.createRelationshipTo( rightNode, SampleRelationshipType.LEFT_TO_RIGHT );
        System.out.println( "Created a relationship from " + leftLabel + " node " + leftNickname + " to " + rightLabel
                + " node " + rightNickname + "" );
        return relationship;
    }

    Relationship getRelationship( int leftNickname, int rightNickname )
    {
        final Node leftNode = findNodeByLabelAndProperty( leftLabel, nicknameProperty, leftNickname );
        final Node rightNode = findNodeByLabelAndProperty( rightLabel, nicknameProperty, rightNickname );
        for ( final Relationship relationship : leftNode.getRelationships( Direction.OUTGOING,
                SampleRelationshipType.LEFT_TO_RIGHT ) )
        {
            if ( relationship.getEndNode().getId() == rightNode.getId() )
            {
                return relationship;
            }
        }
        return null;
    }

    void indexRelationship( Relationship relationship )
    {
        ++currentTime;
        leftToRightIndex.add( relationship, relationshipIndexProperty, new ValueContext( currentTime ).indexNumeric() );
        System.out.println( "Indexed the relationship from "
                + relationship.getStartNode().getProperty( nicknameProperty ) + " to "
                + relationship.getEndNode().getProperty( nicknameProperty ) + " with timestamp " + currentTime );
    }

    void unindexRelationship( Relationship relationship )
    {
        leftToRightIndex.remove( relationship );
        System.out.println( "Unindexed the relationship from "
                + relationship.getStartNode().getProperty( nicknameProperty ) + " to "
                + relationship.getEndNode().getProperty( nicknameProperty ) );
    }

    void createEmptyDb()
    {
        graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            for ( ConstraintDefinition constraint : graphDatabaseService.schema().getConstraints() )
            {
                constraint.drop();
            }
            for ( IndexDefinition index : graphDatabaseService.schema().getIndexes() )
            {
                index.drop();
            }
            for ( Relationship relationship : VersionDifferences.getAllRelationships( graphDatabaseService ) )
            {
                relationship.delete();
            }
            for ( Node node : VersionDifferences.getAllNodes( graphDatabaseService ) )
            {
                node.delete();
            }
            graphDatabaseService.schema().indexFor( leftLabel ).on( nicknameProperty ).create();
            graphDatabaseService.schema().indexFor( rightLabel ).on( nicknameProperty ).create();
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            final IndexManager indexManager = graphDatabaseService.index();
            leftToRightIndex = indexManager.forRelationships( "LEFT_TO_RIGHT" );
            tx.success();
        }
    }

    void createAndIndexRelationship( int leftNickname, int rightNickname )
    {
        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            createANode( leftLabel, leftNickname );
            createANode( rightLabel, rightNickname );
            Relationship relationship = createRelationship( leftNickname, rightNickname );
            indexRelationship( relationship );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    void reindexRelationship( int leftNickname, int rightNickname )
    {
        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            Relationship relationship = getRelationship( leftNickname, rightNickname );
            unindexRelationship( relationship );
            indexRelationship( relationship );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    void verifyReturnedByQuery( int leftNickname, int rightNickname, boolean specifyStartNode )
    {
        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            final Node leftNode = findNodeByLabelAndProperty( leftLabel, nicknameProperty, leftNickname );
            final Node rightNode = findNodeByLabelAndProperty( rightLabel, nicknameProperty, rightNickname );
            boolean theRelationshipWasFound = false;
            for ( final Relationship relationship : leftToRightIndex.query(
                    QueryContext.numericRange( relationshipIndexProperty, Long.MIN_VALUE, Long.MAX_VALUE ).sortNumeric(
                            relationshipIndexProperty, true ), specifyStartNode ? leftNode : null, null ) )
            {
                if ( relationship.getStartNode().getId() == leftNode.getId()
                        && relationship.getEndNode().getId() == rightNode.getId() )
                {
                    theRelationshipWasFound = true;
                    break;
                }
            }
            System.out
                    .println( "The relationship was " + (theRelationshipWasFound ? "" : "NOT ")
                            + "returned by the query when the start node was " + (specifyStartNode ? "" : "NOT ")
                            + "specified" );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    void go()
    {
        createEmptyDb();
        int leftNickname = 1, rightNickname = 2;
        System.out.println( "\nUpdate 1: Create 2 nodes and index their relationship" );
        createAndIndexRelationship( leftNickname, rightNickname );
        verifyReturnedByQuery( leftNickname, rightNickname, false );
        verifyReturnedByQuery( leftNickname, rightNickname, true );
        System.out.println( "\nUpdate 2: Reindex the relationship" );
        reindexRelationship( leftNickname, rightNickname );
        verifyReturnedByQuery( leftNickname, rightNickname, false );
        verifyReturnedByQuery( leftNickname, rightNickname, true );
    }

    public static void main( String[] args )
    {
        new RelationshipIndexProblem().go();
        System.exit( 0 );
    }
}

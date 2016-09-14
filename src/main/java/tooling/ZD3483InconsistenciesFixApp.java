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
package tooling;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class ZD3483InconsistenciesFixApp extends AbstractApp
{
    @Override
    public Continuation execute( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        GraphDatabaseAPI db = ((GraphDatabaseShellServer)getServer()).getDb();
        NeoStores stores = db.getDependencyResolver().resolveDependency( NeoStores.class );
        RelationshipStore relationshipStore = stores.getRelationshipStore();
        Locks locking = db.getDependencyResolver().resolveDependency( Locks.class );
        long n1 = 26656330, n2 = 663261;

        try ( Locks.Client locks = locking.newClient() )
        {
            // Get records and validate them
            RelationshipRecord rel_74430148 = relationshipStore.getRecord( 74430148 );
            RelationshipRecord rel_74430148_orig = rel_74430148.clone();
            validateRelationshipAssumptions( rel_74430148, 26656330, 663261, 2, 74419400, 74430137, 74753399, 74306456 );
            RelationshipRecord rel_74753399 = relationshipStore.getRecord( 74753399 );
            RelationshipRecord rel_74753399_orig = rel_74753399.clone();
            validateRelationshipAssumptions( rel_74753399, 3311437, 663219, 2, 74753402, 74753398, 74766418, 74430148 );
            RelationshipRecord rel_74430152 = relationshipStore.getRecord( 74430152 );
            RelationshipRecord rel_74430152_orig = rel_74430152.clone();
            validateRelationshipAssumptions( rel_74430152, 26656330, 1345167, 2, 74430165, 74430148, 80981547, 51551984 );
            RelationshipRecord actualPrevRelationshipOfTarget_74306456 =
                    findPrev( stores, n2, 74306456, 2, Direction.INCOMING );
            RelationshipRecord actualPrevRelationshipOfTarget_74306456_orig =
                    actualPrevRelationshipOfTarget_74306456.clone();

            // Perform the changes
            rel_74753399.setSecondNextRel( NO_NEXT_RELATIONSHIP.intValue() );
            rel_74430152.setFirstNextRel( NO_NEXT_RELATIONSHIP.intValue() );
            setNext( actualPrevRelationshipOfTarget_74306456, n2, NO_NEXT_RELATIONSHIP.intValue() );

            // Direct update in the stores (not desirable)
    //        relationshipStore.updateRecord( rel_74753399 );
    //        relationshipStore.updateRecord( rel_74430152 );
    //        relationshipStore.updateRecord( actualPrevRelationshipOfTarget_74306456 );

            // Instead wrap this in a TransactionRepresentation and feed to commit process
        }

        return Continuation.INPUT_COMPLETE;
    }

    private static void setNext( RelationshipRecord relationship, long nodeId, long newNextValue )
    {
        boolean found = false;
        if ( nodeId == relationship.getFirstNode() )
        {
            relationship.setFirstNextRel( newNextValue );
            found = true;
        }
        if ( nodeId == relationship.getSecondNode() )
        {
            relationship.setSecondNextRel( newNextValue );
            found = true;
        }
        if ( !found )
        {
            throw new AssertionError( relationship + " is a relationship between two other nodes, not " + nodeId );
        }
    }

    private static RelationshipRecord findPrev( NeoStores stores, long nodeId, long tooFarRelationshipId, int type,
            Direction direction )
    {
        NodeStore nodeStore = stores.getNodeStore();
        NodeRecord node = nodeStore.getRecord( nodeId );
        long relId = node.getNextRel();
        if ( node.isDense() )
        {
            relId = getStartOf( stores, nodeId, relId, type, direction );
        }

        RelationshipStore relationshipStore = stores.getRelationshipStore();
        while ( !NO_NEXT_RELATIONSHIP.is( relId ) )
        {
            RelationshipRecord relationship = relationshipStore.getRecord( relId );
            long nextRelId = getNextTargetOf( nodeId, relationship );
            if ( nextRelId == tooFarRelationshipId )
            {
                return relationship;
            }
        }
        throw new AssertionError( "Expected to find the previous relationship to " + tooFarRelationshipId +
                ", but didn't" );
    }

    private static long getNextTargetOf( long nodeId, RelationshipRecord relationship )
    {
        if ( nodeId == relationship.getFirstNode() )
        {
            return relationship.getFirstNextRel();
        }
        if ( nodeId == relationship.getSecondNode() )
        {
            return relationship.getSecondNextRel();
        }
        throw new AssertionError( "Next relationship " + relationship + " is a relationship between two other nodes" +
                ", not " + nodeId );
    }

    private static long getStartOf( NeoStores stores, long nodeId, long groupId, int type, Direction direction )
    {
        RelationshipGroupStore groupStore = stores.getRelationshipGroupStore();
        while ( !NO_NEXT_RELATIONSHIP.is( groupId ) )
        {
            RelationshipGroupRecord group = groupStore.getRecord( groupId );
            if ( group.getType() == type )
            {
                return getStartOf( group, direction );
            }
            groupId = group.getNext();
        }
        throw new AssertionError( "Expected to find relationships chain head for node:" + nodeId +
                " with type:" + type + " and direction:" + direction );
    }

    private static long getStartOf( RelationshipGroupRecord group, Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING: return group.getFirstOut();
        case INCOMING: return group.getFirstIn();
        case BOTH: return group.getFirstLoop();
        default: throw new IllegalArgumentException( "" + direction );
        }
    }

    private static void validateRelationshipAssumptions( RelationshipRecord relationship,
            long sourceNode, long targetNode, int type,
            long sourcePrev, long sourceNext, long targetPrev, long targetNext )
    {
        validateAssumption( "Source node", sourceNode, relationship.getFirstNode() );
        validateAssumption( "Source prev", sourcePrev, relationship.getFirstPrevRel() );
        validateAssumption( "Source next", sourceNext, relationship.getFirstNextRel() );
        validateAssumption( "Target node", targetNode, relationship.getSecondNode() );
        validateAssumption( "Target prev", targetPrev, relationship.getSecondPrevRel() );
        validateAssumption( "Target next", targetNext, relationship.getSecondNextRel() );
    }

    /**
     * This method is here because this patching application performs small incisions, each one getting
     * a particular record and assuming things about it before making changes. This method will validate
     * those assumptions before making those changes.
     *
     * @param message to print if an assumption didn't validate.
     * @param expectedValue
     * @param actualValue
     */
    private static void validateAssumption( String message, long expectedValue, long actualValue )
    {
        if ( expectedValue != actualValue )
        {
            throw new AssertionError( "False assumption about " + message + ", expected " + expectedValue +
                    " but was " + actualValue );
        }
    }
}

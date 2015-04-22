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
package qa;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class TestLabelEvents
{
    private static enum Labels implements Label
    {
        FIRST,
        SECOND;
    }

    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "the-db" );

        db.registerTransactionEventHandler( labelPrinter() );

        System.out.println( "---" );

        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( Labels.FIRST );
            tx.success();
        }

        System.out.println( "---" );

        try ( Transaction tx = db.beginTx() )
        {
            node.addLabel( Labels.SECOND );
            tx.success();
        }
        catch ( RuntimeException e )
        {
            // OK
        }

        System.out.println( "---" );

        try ( Transaction tx = db.beginTx() )
        {
            node.removeLabel( Labels.FIRST );
            tx.success();
        }
        catch ( RuntimeException e )
        {
            // OK
        }

        System.out.println( "---" );

        try ( Transaction tx = db.beginTx() )
        {
            node.addLabel( Labels.FIRST );
            node.removeLabel( Labels.FIRST );
            db.createNode( Labels.values() );
            tx.success();
        }
        catch ( RuntimeException e )
        {
            // OK
        }
        db.shutdown();
    }

    private static void throwException()
    {
        throw new RuntimeException( "Ignore this" );
    }

    private static TransactionEventHandler<Void> labelPrinter()
    {
        return new TransactionEventHandler<Void>()
        {
            @Override
            public Void beforeCommit( TransactionData data ) throws Exception
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void afterCommit( TransactionData data, Void state )
            {
                printLabels( "commit", data );
            }

            @Override
            public void afterRollback( TransactionData data, Void state )
            {
                printLabels( "rollback", data );
            }
        };
    }

    protected static void printLabels( String string, TransactionData data )
    {
        System.out.println( "Assigned labels (" + string + "):" );
        for ( LabelEntry label : data.assignedLabels() )
        {
            System.out.println( "  " + label );
            System.out.println( "    " + label.label().name() + ", on " + label.node().getId() );
        }

        System.out.println( "Removed labels (" + string + "):" );
        for ( LabelEntry label : data.removedLabels() )
        {
            System.out.println( "  " + label );
            System.out.println( "    " + label.label().name() + ", on " + label.node().getId() );
        }
    }
}

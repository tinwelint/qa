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

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;

public class StartStopDb
{
    public static void main( String[] args ) throws IOException
    {
        GraphDatabaseService db =
                new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( new File( args[0] ) )
//                VersionDifferences.newDbBuilder( args[0] )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE )
                .newGraphDatabase();
        try
        {
//            System.out.println( "Press ENTER to shutdown and exit..." );
//            System.in.read();

            doSomeTransactions( db );

//            something( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void something( GraphDatabaseService db )
    {
        Label label = Label.label( "TestRun" );
        String key = "id";

        try ( Transaction tx = db.beginTx() )
        {
            try ( ResourceIterator<Node> nodes =
                    db.findNodes( label, key, "68bb156b-5399-481c-8516-a45f16ccde62" ) )
            {
                while ( nodes.hasNext() )
                {
                    System.out.println( nodes.next() );
                }
            }
            tx.success();
        }
    }

    private static void doSomeTransactions( GraphDatabaseService db )
    {
        for ( int t = 0; t < 100; t++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    Node node = db.createNode();
                    node.addLabel( DynamicLabel.label( "label-" + i ) );
                    node.setProperty( "key-" + i, i );

                    Relationship relationship = node.createRelationshipTo( node, RelationshipType.withName( "YEAH" ) );
                    relationship.setProperty( "yo", "" + i );
                }
                tx.success();
            }
        }
    }
}

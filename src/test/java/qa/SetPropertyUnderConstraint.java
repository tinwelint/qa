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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.Race;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;

public class SetPropertyUnderConstraint
{
    @Test
    public void shouldTest() throws Throwable
    {
        // GIVEN
        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( clean( "blaj" ) );
        Label label = DynamicLabel.label( "sjdkjk" );
        final String key = "key";
        final String value = "value";
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }

        Node[] nodes = new Node[1000];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
//                nodes[i] = db.createNode( label );
                nodes[i] = db.createNode( new Label[] {label( "1" ), label( "2" ), label( "3" )} );
                nodes[i].setProperty( key, value );
            }
            tx.success();
        }

        Race race = new Race();
        for ( final Node node : nodes )
        {
//            race.addContestant( setProperty( db, key, value, node ) );
            race.addContestant( addLabel( db, label, node ) );
        }
        race.go();

        int count = 0;
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : nodes )
            {
                if ( value.equals( node.getProperty( key, null ) ) && node.hasLabel( label ) )
                {
                    count++;
                }
            }
            tx.success();
        }
        assertEquals( 1, count );
    }

    private Runnable addLabel( final GraphDatabaseService db, final Label label, final Node node )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.addLabel( label );
                    tx.success();
                }
                catch ( ConstraintViolationException e )
                {
                    // OK
                }
            }
        };
    }

    private Runnable setProperty( final GraphDatabaseService db, final String key, final String value, final Node node )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.setProperty( key, value );
                    tx.success();
                }
                catch ( ConstraintViolationException e )
                {
                    // OK
                }
            }
        };
    }

    private String clean( String string ) throws IOException
    {
        FileUtils.deleteRecursively( new File( string ) );
        return string;
    }
}

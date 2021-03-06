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

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.Race;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DeletionRaceTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

    @Test
    public void shouldNotBeAbleToDeleteNodeTwice() throws Throwable
    {
        for ( int r = 0; r < 100; r++ )
        {
            // GIVEN
            Node node = createNode();

            // WHEN
            Race race = new Race();
            AtomicInteger success = new AtomicInteger();
            AtomicInteger failure = new AtomicInteger();
            for ( int i = 0; i < 10; i++ )
            {
                race.addContestant( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            node.delete();
                            tx.success();
                            success.incrementAndGet();
                        }
                        catch ( NotFoundException e )
                        {
                            failure.incrementAndGet();
                        }
                    }
                } );
            }
            race.go();

            // THEN
            assertEquals( 1, success.get() );
            assertEquals( 9, failure.get() );
        }
        fail( "Just so that db is kept" );
    }

    private Node createNode()
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }
        return node;
    }
}

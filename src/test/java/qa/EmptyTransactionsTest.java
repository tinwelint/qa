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

import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.Race;

public class EmptyTransactionsTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldTest() throws Throwable
    {
        // GIVEN
        Race race = new Race();
        for ( int i = 0; i < 100; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    for ( int j = 0; j < 100; j++ )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            tx.success();
                        }
                    }
                }
            } );
        }

        // WHEN
        race.go();
    }
}

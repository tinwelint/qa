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

import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.Race;

public class ZD2489
{
    public final @Rule DatabaseRule dbr = new EmbeddedDatabaseRule();

    @Test
    public void shouldSeeWhereTheExceptionComesFrom() throws Throwable
    {
        while ( true )
        {
            // Set up
            dbr.execute( "CREATE (n:YO)" );

            // Try to trigger
            Race race = new Race();
            race.addContestant( execute( "MATCH (n:YO) SET n.property = 10" ) );
            race.addContestant( execute( "MATCH (n:YO) DELETE n" ) );
            race.go();
        }
    }

    private Runnable execute( final String query )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                dbr.execute( query );
            }
        };
    }
}

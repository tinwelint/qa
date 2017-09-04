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
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ExecutionGuardManualTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() )
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.transaction_timeout, "2s" );
        }
    };

    @Test
    public void shouldTerminateTxAfterTimeout() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            sleep( SECONDS.toMillis( 10 ) );
            db.createNode();
            tx.success();
            System.out.println( "HMM" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldTerminateTxAfterCustomTimeout() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            sleep( SECONDS.toMillis( 7 ) );
            db.createNode();
            tx.success();
            System.out.println( "HMM" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }
}

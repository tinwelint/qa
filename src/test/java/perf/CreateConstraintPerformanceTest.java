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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

import static java.lang.System.currentTimeMillis;

public class CreateConstraintPerformanceTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();
    private final Label label = DynamicLabel.label( "Labulina" );
    private final String key = "key";

    @Test
    public void shouldCreateTheConstraint() throws Exception
    {
        // GIVEN
        Workers<Runnable> workers = new Workers<>( "YEAH" );
        for ( int i = 0; i < 100; i++ )
        {
            final int thread = i;
            workers.start( new Runnable()
            {
                @Override
                public void run()
                {
                    for ( int i = 0; i < 100; i++ )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            for ( int j = 0; j < 1_000; j++ )
                            {
                                db.createNode( label ).setProperty( key, "value-" + thread + "-" + i + "-" + j );
                            }
                            tx.success();
                        }
                    }
                }
            } );
        }
        workers.awaitAndThrowOnError( Exception.class );

        // WHEN
        System.out.println( "Creating constraint" );
        long time = System.currentTimeMillis();
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }
        time = currentTimeMillis()-time;
        System.out.println( time );
    }
}

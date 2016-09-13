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
package perf;

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;
import qa.perf.Performance;
import versiondiff.VersionDifferences;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Uniqueness;

import static qa.perf.Operations.inTx;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class RecordFormatComparisonBenchmark
{
    @Test
    public void nodeStoreScan() throws Exception
    {
        measure( inTx( (on) ->
               {
                   System.out.println( count( on.db.getAllNodes() ) );
               } ) );
    }

    @Test
    public void relationshipStoreScan() throws Exception
    {
        measure( inTx( (on) ->
               {
                   System.out.println( count( VersionDifferences.getAllRelationships( on.db ) ) );
               } ) );
    }

    @Test
    public void randomTraversalsDepthFirst() throws Exception
    {
        Performance.measure( db() )
                   .withAllCores()
                   .withOperations( inTx( (on) -> {
                       count( on.db.traversalDescription()
                           .breadthFirst()
                           .evaluator( Evaluators.toDepth( 3 ) )
                           .uniqueness( Uniqueness.NODE_PATH )
                           .traverse( on.db.getNodeById( ThreadLocalRandom.current().nextInt( 100_000_000 ) ) ) );
                   } ) )
                   .withDuration( 40 )
                   .withBatchSize( 1 )
                   .please();
    }

    private GraphDatabaseTarget db()
    {
        return new GraphDatabaseTarget().onExistingStoreDir( new File( "C:\\Users\\Matilas\\Desktop\\test-subject-busted" ) );
    }

    private void measure( Operation<GraphDatabaseTarget> op ) throws IOException
    {
        GraphDatabaseTarget target = db();
        target.start();
        try
        {
            System.out.println( "warmup" );
            op.perform( target );
            System.out.println( "go" );

            long time = currentTimeMillis();
            op.perform( target );
            time = currentTimeMillis() - time;
            System.out.println( duration( time ) );
        }
        finally
        {
            target.stop();
        }
    }
}

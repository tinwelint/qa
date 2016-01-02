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

import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ReadDeadlocksTest
{
    private enum Labels implements Label
    {
        User, Model;
    }
    private enum Types implements RelationshipType
    {
        in, out;
    }

    private static final int NUM_USERS = 1_000;
    private static final int MODELS_PER_USER = 10;
    private static final int CONNECTIONS_PER_MODEL = 4;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void shouldProduceThoseDeadlocks() throws Exception
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();
        initialDataset( cluster.getMaster() );
        cluster.sync();
        final String query =
                "MATCH (user:User {id: {userId}}) " +
                "MATCH (other:Model) " +
                "WHERE other.id IN {otherIds} " +
                "OPTIONAL MATCH (user) <-[in]- (other) " +
                "OPTIONAL MATCH (user) -[out]-> (other) " +
                "RETURN other.id AS otherId, " +
                "COLLECT(TYPE(in)) AS inTypes, COLLECT(in) AS inProps, " +
                "COLLECT(TYPE(out)) AS outTypes, COLLECT(out) AS outProps";

        // WHEN
        System.out.println( "Starting load" );
        final AtomicBoolean end = new AtomicBoolean();
        final GraphDatabaseService db = cluster.getAnySlave();
        Workers<Runnable> workers = new Workers<>( getClass().getSimpleName() );
        for ( int i = 0; i < 10; i++ )
        {
            workers.start( new Runnable()
            {
                @Override
                public void run()
                {
                    Map<String,Object> parameters = new HashMap<>();
                    Random random = ThreadLocalRandom.current();
                    int deadlocks = 0, successes = 0, otherFailures = 0;
                    while ( !end.get() )
                    {
                        boolean success = true;
                        try ( Transaction tx = db.beginTx() )
                        {
                            parameters.put( "userId", random.nextInt( NUM_USERS * 2 ) );
                            parameters.put( "otherIds", randomModelIds( random ) );
                            Result result = db.execute( query, parameters );
                            IteratorUtil.count( result );
                            result.close();
                            tx.success();
                        }
                        catch ( DeadlockDetectedException e )
                        {
                            deadlocks++;
                            success = false;
                        }
                        catch ( Exception e )
                        {
                            otherFailures++;
                            success = false;
                        }
                        finally
                        {
                            if ( success )
                            {
                                successes++;
                            }
                        }
                    }

                    System.out.println( "deadlocks:" + deadlocks + ", successes:" + successes + ", other:" + otherFailures );
                }

                private Object randomModelIds( Random random )
                {
                    int maxModels = NUM_USERS * MODELS_PER_USER * 2;
                    int[] result = new int[random.nextInt( 10 )+1];
                    for ( int i = 0; i < result.length; i++ )
                    {
                        result[i] = random.nextInt( maxModels );
                    }
                    return result;
                }
            } );
        }

        Thread.sleep( MINUTES.toMillis( 1 ) );
        end.set( true );
        workers.awaitAndThrowOnError( RuntimeException.class );
    }

    private void initialDataset( GraphDatabaseService db )
    {
        // Constraint
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( Labels.User ).assertPropertyIsUnique( "id" ).create();
            db.schema().constraintFor( Labels.Model ).assertPropertyIsUnique( "id" ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 30, SECONDS );
            tx.success();
        }
        System.out.println( "Constraint built" );

        // Users
        Node[] users = new Node[NUM_USERS];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < users.length; i++ )
            {
                Node user = db.createNode( Labels.User );
                user.setProperty( "id", i );
                users[i] = user;
            }
            tx.success();
        }
        System.out.println( "Users built" );

        // Models
        Node[] models = new Node[users.length * MODELS_PER_USER];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < models.length; i++ )
            {
                Node model = db.createNode( Labels.Model );
                model.setProperty( "id", i );
                models[i] = model;
            }
            tx.success();
        }
        System.out.println( "Models built" );

        // Connect out
        int connections = models.length * CONNECTIONS_PER_MODEL/2;
        Random random = ThreadLocalRandom.current();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < connections; i++ )
            {
                users[random.nextInt( users.length )].createRelationshipTo(
                        models[random.nextInt( models.length )], Types.out );
            }
            tx.success();
        }
        System.out.println( "Connections out built" );

        // Connect in
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < connections; i++ )
            {
                models[random.nextInt( models.length )].createRelationshipTo(
                        users[random.nextInt( users.length )], Types.in );
            }
            tx.success();
        }
        System.out.println( "Connections in built" );
    }
}

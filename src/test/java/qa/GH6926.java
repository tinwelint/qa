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

import org.junit.Test;
import versiondiff.VersionDifferences;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.test.Race;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertNotNull;

import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class GH6926
{
    private final Label label = VersionDifferences.label( "Impression" );
    private final String key = "hash";
    private final String query =
            "MERGE (a:" + label.name() + " {" + key + ":{hash}}) ON CREATE SET a:Test ON MATCH SET a:Test";

    @Test
    public void shouldTryAndBreakConstraint() throws Throwable
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(
                new File( "K:\\var\\lib\\neo4j\\data\\graph.db" ) );
        try
        {
//            try ( Transaction tx = db.beginTx() )
//            {
//                db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
//                tx.success();
//            }
//            try ( Transaction tx = db.beginTx() )
//            {
//                db.schema().awaitIndexesOnline( 10, SECONDS );
//                tx.success();
//            }
            try ( Transaction tx = db.beginTx() )
            {
                for ( ConstraintDefinition constraint : db.schema().getConstraints() )
                {
                    System.out.println( constraint );
                }
                tx.success();
            }

            List<Label> labels;
            try ( Transaction tx = db.beginTx() )
            {
                labels = asList( GlobalGraphOperations.at( db ).getAllLabels() );
                tx.success();
            }
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while ( true )
            {
                String hash = randomHash( random );
                raceMergeCreate( db, hash, labels, random );
                try ( Transaction tx = db.beginTx() )
                {
                    assertNotNull( "Couldn't find node " + label + " " + hash, db.findNode( label, key, hash ) ); // <-- will also assert there aren't multiple
                    tx.success();
                }
                System.out.println( "Completed race for " + hash );
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private static final char[] HEX_CHARS =
            new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private String randomHash( ThreadLocalRandom random )
    {
        char[] chars = new char[31];
        for ( int i = 0; i < chars.length; i++ )
        {
            chars[i] = HEX_CHARS[random.nextInt( HEX_CHARS.length )];
        }
        return new String( chars );
    }

    @Test
    public void shouldSeeHowTheRealDbLooksLike() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File(
                "K:\\var\\lib\\neo4j\\data\\graph.db" ) );
        try ( Transaction tx = db.beginTx() )
        {
            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                System.out.println( constraint );
            }
            tx.success();
        }

        try ( Result result = db.execute( "MATCH (a:Impression {hash:'e612b20ac8dc47b1b08e1fbc67b5a3a2'}) RETURN a" ) )
        {
            while ( result.hasNext() )
            {
                Map<String,Object> row = result.next();
                Node node = (Node) row.get( "a" );
                System.out.println( node );
                db.getNodeById( node.getId() );
                System.out.println( "  ok exists" );
                if ( node.hasLabel( label ) )
                {
                    System.out.println( "  has that label" );
                }
                if ( node.getProperty( key, "" ).equals( "e612b20ac8dc47b1b08e1fbc67b5a3a2" ) )
                {
                    System.out.println( "  has that hash" );
                }
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private void raceMergeCreate( GraphDatabaseService db, String hash, List<Label> labels,
            ThreadLocalRandom random ) throws Throwable
    {
        Race race = new Race();
        int contestants = ThreadLocalRandom.current().nextInt( 2, 100 );
        for ( int i = 0; i < contestants; i++ )
        {
            race.addContestant( () ->
            {
                while ( true )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
//                        db.execute( query, map( "hash", hash ) );

                        try
                        {
                            db.createNode( label ).setProperty( key, hash );
                        }
                        catch ( ConstraintViolationException e1 )
                        {
                            break;
                        }
                        int stuff = random.nextInt( 1, 200 );
                        for ( int j = 0; j < stuff; j++ )
                        {
                            try
                            {
                                db.createNode( labels.get( random.nextInt( labels.size() ) ) ).setProperty( key, hash );
                            }
                            catch ( ConstraintViolationException e )
                            {
                                // fine
                            }
                        }

                        tx.success();
                        break;
                    }
                    catch ( DeadlockDetectedException e )
                    {
                        // This is fine
                    }
                }
            } );
        }
        race.go();
    }
}

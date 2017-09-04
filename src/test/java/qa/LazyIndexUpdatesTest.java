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

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.RandomRule;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.single;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.test.TargetDirectory.testDirForTest;

public class LazyIndexUpdatesTest
{
    private static final String[] KEYS = {"key", "key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7"};

    private static enum Labels implements Label
    {
        ONE, TWO( true ), THREE, FOUR( true ), FIVE, SIX, SEVEN;

        private final boolean hasConstraint;

        private Labels()
        {
            this( false );
        }

        private Labels( boolean hasConstraint )
        {
            this.hasConstraint = hasConstraint;
        }
    }

    public final @Rule TestDirectory directory = testDirForTest( getClass() );
    public final @Rule RandomRule random = new RandomRule();
    public final @Rule RepeatRule repeat = new RepeatRule();
    private final String mainKey = KEYS[0];
    private int highestNodeId;

    @Repeat( times = 100 )
    @Test
    public void shouldGetCorrectUpdates() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        for ( Labels label : Labels.values() )
        {
            if ( label.hasConstraint )
            {
                createUniquenessConstraint( db, label );
            }
        }
        createLotsOfSchemaNoise( db );
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                createNewNode( db );
            }
            tx.success();
        }

        // WHEN
        boolean worrying = false;
        for ( int i = 0; i < 1000; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                int txSize = random.intBetween( 1, 3 );
                for ( int j = 0; j < txSize; j++ )
                {
                    try
                    {
                        makeRandomChange( db );
                    }
                    catch ( ConstraintViolationException e )
                    {
                        // OK
                    }
                    tx.success();
                }
            }
            try ( Transaction tx = db.beginTx() )
            {
                try
                {
                    verify( db );
                }
                catch ( NotFoundException e )
                {
                    // Breaks here since this might actually be the cause of the constraints inconsistencies
                    // thing. The index updates rely on reading node properties from the store in some scenarios
                    // and that will be wrong if we can't find the properties in the store.

                    e.printStackTrace();
                    System.out.println( "Found something weird, let's CC have a crack at it right away" );
                    worrying = true;
                    break;
                }
                tx.success();
            }

        }
        db.shutdown();

        Result result = new ConsistencyCheckService().runFullConsistencyCheck( directory.directory(),
                new Config(), NONE, NullLogProvider.getInstance(), false );
        assertTrue( result.isSuccessful() );
        System.out.println( "OK" + (worrying ? " although worrying" : "") );
        assertFalse( worrying );
    }

    private void makeRandomChange( GraphDatabaseService db )
    {
        float thing = random.nextFloat();
        if ( thing < 0.05 )
        {
            createNewNode( db );
        }
        else if ( thing < 0.3 )
        {
            setPropertiesOnNode( db );
        }
        else if ( thing < 0.7 )
        {
            addLabelsToNode( db );
        }
        else
        {
            removeLabelsFromNode( db );
        }
    }

    private void setPropertiesOnNode( GraphDatabaseService db )
    {
        Node node = randomNode( db );
        int props = random.intBetween( 1, 2 );
        for ( int i = 0; i < props; i++ )
        {
            node.setProperty( random.among( KEYS ), random.propertyValue() );
        }
    }

    private void createNewNode( GraphDatabaseService db )
    {
        Node node = db.createNode( random.selection( Labels.values(), 0, 3, false ) );
        try
        {
            node.setProperty( mainKey, node.getId() );
        }
        catch ( ConstraintViolationException e )
        {
            throw new RuntimeException( e );
        }
        highestNodeId = (int) node.getId();
    }

    private void addLabelsToNode( GraphDatabaseService db )
    {
        Node node = randomNode( db );
        for ( Label label : random.selection( Labels.values(), 1, 2, false ) )
        {
            node.addLabel( label );
        }
    }

    private Node randomNode( GraphDatabaseService db )
    {
        return db.getNodeById( random.intBetween( 0, highestNodeId ) );
    }

    private void removeLabelsFromNode( GraphDatabaseService db )
    {
        Node node = randomNode( db );
        for ( Label label : random.selection( Labels.values(), 1, 4, false ) )
        {
            node.removeLabel( label );
        }
    }

    private void verify( GraphDatabaseService db ) throws IndexNotFoundKernelException
    {
        @SuppressWarnings( "deprecation" )
        IndexingService indexing = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(
                IndexingService.class );
        long indexId = 1;
        for ( Labels label : Labels.values() )
        {
            if ( label.hasConstraint )
            {
                IndexProxy index = indexing.getIndexProxy( indexId ); // we're guessing the index id here, but hey
                try ( IndexReader reader = index.newReader() )
                {
                    verifyIndexWithLabel( db, reader, label );
                }
                indexId += 2; // index, constraint, index, constraint,...
            }
        }
    }

    private void verifyIndexWithLabel( GraphDatabaseService db, IndexReader reader, Label label )
    {
        for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
        {
            verifyNode( node, reader, label );
        }
    }

    private void verifyNode( Node node, IndexReader reader, Label label )
    {
        if ( node.hasLabel( label ) )
        {
            assertEquals( node.getId(), single( reader.seek( node.getProperty( mainKey ) ) ) );
        }
    }

    private void createUniquenessConstraint( GraphDatabaseService db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( mainKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }

    private void createLotsOfSchemaNoise( GraphDatabaseService db )
    {
        for ( int i = 0; i < 50; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Label label = random.among( Labels.values() );
                String key = random.among( KEYS );
                if ( random.nextBoolean() )
                {
                    db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
                }
                else
                {
                    db.schema().indexFor( label ).on( key ).create();
                }
                tx.success();
            }
            catch ( ConstraintViolationException e )
            {
                // It's OK, we're creating lots of random rules here, some are bound to be the same
            }
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 100, SECONDS );
            tx.success();
        }
    }
}

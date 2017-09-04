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
import versiondiff.VersionDifferences;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static org.junit.Assert.assertEquals;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.single;
import static org.neo4j.unsafe.impl.batchimport.input.SimpleInputIteratorWrapper.wrap;

public class GoBeyondLuceneIdLimitsTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( GoBeyondLuceneIdLimitsTest.class );
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final Label label = VersionDifferences.label( "Label" );
    private final String key = "key";
    private final long base = Integer.MAX_VALUE;
    private final long additional = 10_000_000;
    private final long nodeCount = base + additional;

    @Test
    public void shouldVerifyUniqueness() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < 500_000; )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int j = 0; j < 80_000; j++, i++ )
                {
                    Object value = i % 45123 == 0 ? 600_000 : i;
                    db.createNode( label ).setProperty( key, value );
                }
                tx.success();
            }
            System.out.println( "tx" );
        }

        System.out.println( "1" );
        try ( Transaction tx = db.beginTx() )
        {
            // WHEN
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }
        // all good
    }

    @Test
    public void shouldDoItWithImportTool() throws Exception
    {
        db.shutdown();

        // GIVEN
        BatchImporter importer = new ParallelBatchImporter( directory.directory(), Configuration.DEFAULT,
                NullLogService.getInstance(),
                ExecutionMonitors.defaultVisible()
//                , new Config()
                );
        importer.doImport( justNodes() );
        db.ensureStarted();

        // WHEN
        createIndex();

        // THEN
        verifyIndex();
    }

    @Test
    public void shouldDoItWithSingleThread() throws Exception
    {
        // GIVEN
        for ( long i = 0; i < nodeCount; )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int b = 0; b < 1_000_000; b++, i++ )
                {
                    db.createNode( label ).setProperty( key, i );
                }
                tx.success();
            }
            System.out.println( Format.bytes( i ) );
        }
        createIndex();

        // THEN
        verifyIndex();
    }

    @Test
    public void shouldDoItDirectlyAtopIndexAccessor() throws Exception
    {
        // GIVEN
        SchemaIndexProvider provider = new LuceneSchemaIndexProvider( new DefaultFileSystemAbstraction(),
                DirectoryFactory.PERSISTENT, directory.directory() );
        System.out.println( directory.directory() );
        IndexConfiguration indexConfig = VersionDifferences.nonUniqueIndexConfiguration();
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = provider.getPopulator( 0, new IndexDescriptor( 0, 0 ), indexConfig, samplingConfig );

        // WHEN
        long nodeCount = 10_000_000;
        populator.create();
        for ( long i = 0; i < nodeCount; )
        {
            List<NodePropertyUpdate> updates = new ArrayList<>();
            long[] labels = new long[] {0};
            for ( int j = 0; j < 1_000_000; j++, i++ )
            {
                updates.add( NodePropertyUpdate.add( i, 0, i, labels ) );
            }
            populator.add( updates );
            System.out.println( i );
        }
        populator.close( true );

        // THEN
        try ( IndexAccessor index = provider.getOnlineAccessor( 0, indexConfig, samplingConfig ) )
        {
            try ( IndexReader reader = index.newReader() )
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                for ( int i = 0; i < 10_000; i++ )
                {
                    long id = random.nextLong( nodeCount );
                    assertEquals( id, single( reader.seek( id ) ) );
                }
            }
        }
    }

    private void verifyIndex()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 100; i++ )
        {
            long id = random.nextLong( nodeCount );
            assertEquals( id, db.findNode( label, key, id ).getId() );
        }
        for ( int i = 0; i < 100; i++ )
        {
            long id = base + random.nextLong( additional );
            assertEquals( id, db.findNode( label, key, id ).getId() );
        }
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( key ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }

    private Input justNodes()
    {
        InputIterable<InputNode> nodes = wrap( "Blah", () -> new PrefetchingIterator<InputNode>()
        {
            final String[] labels = {label.name()};
            long i;

            @Override
            protected InputNode fetchNextOrNull()
            {
                if ( i >= nodeCount )
                {
                    return null;
                }
                return new InputNode( "Blah", i, i, i, new Object[] {key, i++}, null, labels, null );
            }
        } );
        InputIterable<InputRelationship> relationships = wrap( "not", () -> IteratorUtil.emptyIterator() );
        return Inputs.input( nodes, relationships, IdMappers.actual(), IdGenerators.fromInput(), false,
                new BadCollector( System.out, 0, 0 ) );
    }
}

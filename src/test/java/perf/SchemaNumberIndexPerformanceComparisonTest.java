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

import java.io.File;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndexProvider;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Values;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.add;

public class SchemaNumberIndexPerformanceComparisonTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldTest() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap( pagecache_memory.name(), "6G" ) );
        File storeDir = new File( "C:/Users/Matilas/Desktop/gdb" );
        storeDir.mkdirs();
        Stream.of( storeDir.listFiles() ).forEach( file -> file.delete() );
        try ( PageCache pageCache = new ConfiguringPageCacheFactory( new DefaultFileSystemAbstraction(),
                config, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, NullLog.getInstance() ).getOrCreatePageCache() )
        {
            SchemaIndexProvider providah = new NativeSchemaNumberIndexProvider( pageCache, storeDir, NullLogProvider.getInstance(),
                    RecoveryCleanupWorkCollector.IMMEDIATE, false );
//            SchemaIndexProvider providah = new LuceneSchemaIndexProvider( new DefaultFileSystemAbstraction(),
//                    DirectoryFactory.PERSISTENT, storeDir, NullLogProvider.getInstance(), config, OperationalMode.single );
            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 0, 0 );
            try ( IndexAccessor index = providah.getOnlineAccessor( 0, descriptor, new IndexSamplingConfig( config ) ) )
            {
                long time = currentTimeMillis();
                int count = 100_000_000;
                int lap = count / 100;
                try ( IndexUpdater updater = index.newUpdater( IndexUpdateMode.ONLINE ) )
                {
                    for ( int i = 0; i < count; i++ )
                    {
                        int value =
//                                count - i;
                                i;
//                                random.nextInt();
                        updater.process( add( 0, descriptor, Values.intValue( value ) ) );
                        if ( i % lap == 0 && i > 0 )
                        {
                            System.out.println( "@" + i );
                        }
                    }
                }
                time = currentTimeMillis() - time;
                double insertsPerMs = (double) count / time;
                System.out.println( "End " + duration( time ) + " " + insertsPerMs + " inserts/ms" );
                index.force();
            }
        }
    }

    @Test
    public void shouldRemove() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap( pagecache_memory.name(), "6G" ) );
        File storeDir = new File( "C:/Users/Matilas/Desktop/gdb" );
        storeDir.mkdirs();
        try ( PageCache pageCache = new ConfiguringPageCacheFactory( new DefaultFileSystemAbstraction(),
                config, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, NullLog.getInstance() ).getOrCreatePageCache() )
        {
            SchemaIndexProvider providah = new NativeSchemaNumberIndexProvider( pageCache, storeDir, NullLogProvider.getInstance(),
                    RecoveryCleanupWorkCollector.IMMEDIATE, false );
            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 0, 0 );
            try ( IndexAccessor index = providah.getOnlineAccessor( 0, descriptor, new IndexSamplingConfig( config ) ) )
            {
                try ( IndexUpdater updater = index.newUpdater( IndexUpdateMode.RECOVERY ) )
                {
                    PrimitiveLongSet nodeIds = Primitive.longSet();
                    nodeIds.add( 10 );
                    updater.remove( nodeIds );
                }
            }
        }
    }
}

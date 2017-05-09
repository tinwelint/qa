/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tooling;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Args.Option;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.Strings.TAB;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT_MAX_MEMORY_PERCENT;
import static org.neo4j.unsafe.impl.batchimport.Configuration.calculateMaxMemoryFromPercent;
import static org.neo4j.unsafe.impl.batchimport.Configuration.canDetectFreeMemory;

/**
 * User-facing command line tool around a {@link BatchImporter}.
 */
public class PageCacheDiagnosticsTool
{
    private static final class SingleThreadReader implements LongSupplier
    {
        private final RelationshipStore store;
        private final long highId;
        private final boolean read;

        public SingleThreadReader( RelationshipStore store, boolean read )
        {
            this.store = store;
            this.read = read;
            this.highId = store.getHighId();
        }

        @Override
        public long getAsLong()
        {
            long count = 0;
            try ( RecordCursor<RelationshipRecord> cursor = store.newRecordCursor( store.newRecord(), read )
                    .acquire( 0, RecordLoad.CHECK ) )
            {
                for ( long i = 0; i < highId; i++ )
                {
                    if ( cursor.next( i ) )
                    {
                        count++;
                    }
                }
            }
            return count;
        }
    }

    private static final class MultiThreadReader implements LongSupplier
    {
        private final int stride;
        private final Configuration configuration;
        private final RelationshipStore store;
        private final int recordsPerPage;
        private final long highPageId;
        private final boolean read;

        public MultiThreadReader( int stride, Configuration configuration, RelationshipStore store, boolean read )
        {
            this.stride = stride;
            this.configuration = configuration;
            this.store = store;
            this.read = read;
            this.recordsPerPage = store.getRecordsPerPage();
            this.highPageId = store.getHighId() / recordsPerPage;
        }

        @Override
        public long getAsLong()
        {
            AtomicLong nextPageId = new AtomicLong();
            ExecutorService executor = Executors.newFixedThreadPool( configuration.maxNumberOfProcessors() );
            for ( int i = 0; i < configuration.maxNumberOfProcessors(); i++ )
            {
                executor.submit( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try ( RecordCursor<RelationshipRecord> cursor = store.newRecordCursor( store.newRecord(), read )
                                .acquire( 0, RecordLoad.CHECK ) )
                        {
                            while ( true )
                            {
                                long pageId = nextPageId.addAndGet( stride );
                                if ( pageId >= highPageId )
                                {
                                    break;
                                }

                                long id = pageId * recordsPerPage;
                                for ( int p = 0; p < stride && pageId + p < highPageId; p++ )
                                {
                                    for ( int j = 0; j < recordsPerPage; j++ )
                                    {
                                        cursor.next( id++ );
                                    }
                                }
                            }
                        }
                        catch ( Throwable t )
                        {
                            t.printStackTrace();
                        }
                    }
                } );
            }
            executor.shutdown();
            try
            {
                executor.awaitTermination( 1, TimeUnit.DAYS );
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
                throw new RuntimeException( e );
            }
            return 0;
        }
    }

    enum Options
    {
        STORE_DIR( "into", null,
                "<store-dir>",
                "Database directory to import into. " + "Must not contain existing database." ),
        PROCESSORS( "processors", null,
                "<max processor count>",
                "(advanced) Max number of processors used by the importer. Defaults to the number of "
                        + "available processors reported by the JVM"
                        + availableProcessorsHint()
                        + ". There is a certain amount of minimum threads needed so for that reason there "
                        + "is no lower bound for this value. For optimal performance this value shouldn't be "
                        + "greater than the number of available processors." ),
        MAX_MEMORY( "max-memory", null,
                "<max memory that importer can use>",
                "(advanced) Maximum memory that importer can use for various data structures and caching " +
                "to improve performance. If left as unspecified (null) it is set to " + DEFAULT_MAX_MEMORY_PERCENT +
                "% of (free memory on machine - max JVM memory). " +
                "Values can be plain numbers, like 10000000 or e.g. 20G for 20 gigabyte, or even e.g. 70%." );

        private final String key;
        private final Object defaultValue;
        private final String usage;
        private final String description;
        private final boolean keyAndUsageGoTogether;
        private final boolean supported;

        Options( String key, Object defaultValue, String usage, String description )
        {
            this( key, defaultValue, usage, description, false, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported )
        {
            this( key, defaultValue, usage, description, supported, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported, boolean
                keyAndUsageGoTogether
                )
        {
            this.key = key;
            this.defaultValue = defaultValue;
            this.usage = usage;
            this.description = description;
            this.supported = supported;
            this.keyAndUsageGoTogether = keyAndUsageGoTogether;
        }

        String key()
        {
            return key;
        }

        String argument()
        {
            return "--" + key();
        }

        void printUsage( PrintStream out )
        {
            out.println( argument() + spaceInBetweenArgumentAndUsage() + usage );
            for ( String line : Args.splitLongLine( descriptionWithDefaultValue().replace( "`", "" ), 80 ) )
            {
                out.println( "\t" + line );
            }
        }

        private String spaceInBetweenArgumentAndUsage()
        {
            return keyAndUsageGoTogether ? "" : " ";
        }

        String descriptionWithDefaultValue()
        {
            String result = description;
            if ( defaultValue != null )
            {
                if ( !result.endsWith( "." ) )
                {
                    result += ".";
                }
                result += " Default value: " + defaultValue;
            }
            return result;
        }

        String manPageEntry()
        {
            String filteredDescription = descriptionWithDefaultValue().replace( availableProcessorsHint(), "" );
            String usageString = (usage.length() > 0) ? spaceInBetweenArgumentAndUsage() + usage : "";
            return "*" + argument() + usageString + "*::\n" + filteredDescription + "\n\n";
        }

        String manualEntry()
        {
            return "[[import-tool-option-" + key() + "]]\n" + manPageEntry() + "//^\n\n";
        }

        Object defaultValue()
        {
            return defaultValue;
        }

        private static String availableProcessorsHint()
        {
            return " (in your case " + Runtime.getRuntime().availableProcessors() + ")";
        }

        public boolean isSupportedOption()
        {
            return this.supported;
        }
    }

    /**
     * Delimiter used between files in an input group.
     */
    static final String MULTI_FILE_DELIMITER = ",";

    /**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     */
    public static void main( String[] incomingArguments ) throws IOException
    {
        main( incomingArguments, false );
    }

    /**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     * @param defaultSettingsSuitableForTests default configuration geared towards unit/integration
     * test environments, for example lower default buffer sizes.
     */
    public static void main( String[] incomingArguments, boolean defaultSettingsSuitableForTests ) throws IOException
    {
        System.err.println("WARNING: neo4j-import is deprecated and support for it will be removed in a future\n" +
                "version of Neo4j; please use neo4j-admin import instead.\n");
        PrintStream out = System.out;
        PrintStream err = System.err;
        Args args = Args.parse( incomingArguments );

        if ( ArrayUtil.isEmpty( incomingArguments ) || asksForUsage( args ) )
        {
            printUsage( out );
            return;
        }

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File storeDir;
        Number processors = null;
        org.neo4j.unsafe.impl.batchimport.Configuration configuration = null;
        Long maxMemory = null;

        boolean success = false;
        try
        {
            storeDir = args.interpretOption( Options.STORE_DIR.key(), Converters.<File>mandatory(),
                    Converters.toFile() );
            Config config = Config.defaults();
            config.augment( stringMap( GraphDatabaseSettings.neo4j_home.name(), storeDir.getAbsolutePath() ) );
            String maxMemoryString = args.get( Options.MAX_MEMORY.key(), null );
            maxMemory = parseMaxMemory( maxMemoryString );
            processors = args.getNumber( Options.PROCESSORS.key(), null );
            configuration = importConfiguration( processors, defaultSettingsSuitableForTests, maxMemory );
            String what = args.get( "what", "" );
            doIt( storeDir, configuration, what );

            success = true;
        }
        catch ( IllegalArgumentException e )
        {
            throw andPrintError( "Input error", e, false, err );
        }
        catch ( IOException e )
        {
            throw andPrintError( "File error", e, false, err );
        }
    }

    private static void doIt( File storeDir, Configuration configuration, String what ) throws IOException
    {
        SingleFilePageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        swapper.setFileSystemAbstraction( fs );
        long memory = configuration.pageCacheMemory();
        int pageSize = (int) kibiBytes( 8 );
        try ( PageCache pageCache = new MuninnPageCache( swapper, (int) (memory / pageSize), pageSize, PageCacheTracer.NULL );
            NeoStores stores = new StoreFactory( storeDir, pageCache, fs, NullLogProvider.getInstance() ).openNeoStores( StoreType.RELATIONSHIP ) )
        {
            stores.makeStoreOk();
            RelationshipStore store = stores.getRelationshipStore();
            long highId = store.getHighId();
            int recordsPerPage = store.getRecordsPerPage();
            long highPageId = highId / recordsPerPage;

            if ( what.contains( "1" ) )
            measure( "Single thread plow pages", () ->
            {
                try
                {
                    long count = 0;
                    try ( PageCursor cursor =
                            store.getPagedFile().io( 0, PagedFile.PF_SHARED_READ_LOCK | PagedFile.PF_READ_AHEAD ) )
                    {
                        for ( long i = 0; i < highPageId; i++ )
                        {
                            if ( cursor.next( i ) )
                            {
                                count++;
                            }
                        }
                    }
                    return count;
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            } );
            if ( what.contains( "2" ) )
            measure( "Single thread read bytes", () ->
            {
                try
                {
                    long count = 0;
                    byte[] bytes = new byte[pageCache.pageSize()];
                    try ( PageCursor cursor =
                            store.getPagedFile().io( 0, PagedFile.PF_SHARED_READ_LOCK | PagedFile.PF_READ_AHEAD ) )
                    {
                        for ( long i = 0; i < highPageId; i++ )
                        {
                            if ( cursor.next( i ) )
                            {
                                do
                                {
                                    cursor.getBytes( bytes );
                                }
                                while ( cursor.shouldRetry() );
                                count++;
                            }
                        }
                    }
                    return count;
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            } );
            if ( what.contains( "3" ) )
            measure( "Single thread read records", new SingleThreadReader( store, true ) );
            if ( what.contains( "4" ) )
            measure( "Multiple threads read records, 1 page each", new MultiThreadReader( 1, configuration, store, true ) );
            if ( what.contains( "5" ) )
            measure( "Multiple threads read records, 100 pages each", new MultiThreadReader( 100, configuration, store, true ) );
            if ( what.contains( "6" ) )
            measure( "W Single thread read records", new SingleThreadReader( store, false ) );
            if ( what.contains( "7" ) )
            measure( "W Multiple threads read records, 1 page each", new MultiThreadReader( 1, configuration, store, false ) );
            if ( what.contains( "8" ) )
            measure( "W Multiple threads read records, 100 pages each", new MultiThreadReader( 100, configuration, store, false ) );
        }
    }

    private static void measure( String name, LongSupplier thing )
    {
        System.out.print( "Measuring " + name );
        long time = currentTimeMillis();
        long count = thing.getAsLong();
        time = currentTimeMillis() - time;
        System.out.println( ": " + count + " in " + duration( time ) );
    }

    private static Long parseMaxMemory( String maxMemoryString )
    {
        if ( maxMemoryString != null )
        {
            maxMemoryString = maxMemoryString.trim();
            if ( maxMemoryString.endsWith( "%" ) )
            {
                int percent = Integer.parseInt( maxMemoryString.substring( 0, maxMemoryString.length() - 1 ) );
                long result = calculateMaxMemoryFromPercent( percent );
                if ( !canDetectFreeMemory() )
                {
                    System.err.println( "WARNING: amount of free memory couldn't be detected so defaults to " +
                            bytes( result ) + ". For optimal performance instead explicitly specify amount of " +
                            "memory that importer is allowed to use using " + Options.MAX_MEMORY.argument() );
                }
                return result;
            }
            return Settings.parseLongWithUnit( maxMemoryString );
        }
        return null;
    }

    public static void doImport( PrintStream out, PrintStream err, File storeDir, File logsDir, File badFile,
                                 FileSystemAbstraction fs, Collection<Option<File[]>> nodesFiles,
                                 Collection<Option<File[]>> relationshipsFiles, boolean enableStacktrace, Input input,
                                 Config dbConfig, OutputStream badOutput,
                                 org.neo4j.unsafe.impl.batchimport.Configuration configuration ) throws IOException
    {
        boolean success;
        LifeSupport life = new LifeSupport();

        LogService logService = life.add( StoreLogService.inLogsDirectory( fs, logsDir ) );

        life.start();
        BatchImporter importer = new ParallelBatchImporter( storeDir,
                configuration,
                logService,
                ExecutionMonitors.defaultVisible(),
                dbConfig );
        success = false;
        try
        {
            importer.doImport( input );
            success = true;
        }
        catch ( Exception e )
        {
            throw andPrintError( "Import error", e, enableStacktrace, err );
        }
        finally
        {
            Collector collector = input.badCollector();
            int numberOfBadEntries = collector.badEntries();
            collector.close();
            IOUtils.closeAll( badOutput );

            if ( badFile != null )
            {
                if ( numberOfBadEntries > 0 )
                {
                    System.out.println( "There were bad entries which were skipped and logged into " +
                            badFile.getAbsolutePath() );
                }
            }

            life.shutdown();
            if ( !success )
            {
                try
                {
                    StoreFile.fileOperation( FileOperation.DELETE, fs, storeDir, null,
                            Iterables.<StoreFile,StoreFile>iterable( StoreFile.values() ),
                            false, ExistingTargetStrategy.FAIL, StoreFileType.values() );
                }
                catch ( IOException e )
                {
                    err.println( "Unable to delete store files after an aborted import " + e );
                    if ( enableStacktrace )
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static org.neo4j.unsafe.impl.batchimport.Configuration importConfiguration( final Number processors,
            final boolean defaultSettingsSuitableForTests )
    {
        return importConfiguration( processors, defaultSettingsSuitableForTests, null );
    }

    public static org.neo4j.unsafe.impl.batchimport.Configuration importConfiguration( final Number processors,
            final boolean defaultSettingsSuitableForTests, Long maxMemory )
    {
        return new org.neo4j.unsafe.impl.batchimport.Configuration()
        {
            @Override
            public long pageCacheMemory()
            {
                return defaultSettingsSuitableForTests ? mebiBytes( 8 ) : DEFAULT.pageCacheMemory();
            }

            @Override
            public int maxNumberOfProcessors()
            {
                return processors != null ? processors.intValue() : DEFAULT.maxNumberOfProcessors();
            }

            @Override
            public long maxMemoryUsage()
            {
                return maxMemory != null ? maxMemory.longValue() : DEFAULT.maxMemoryUsage();
            }
        };
    }

    /**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     * @param stackTrace whether or not to also print the stack trace of the error.
     * @param err
     */
    private static RuntimeException andPrintError( String typeOfError, Exception e, boolean stackTrace,
            PrintStream err )
    {
        printErrorMessage( typeOfError + ": " + e.getMessage(), e, true, err );
        err.println();

        // Mute the stack trace that the default exception handler would have liked to print.
        // Calling System.exit( 1 ) or similar would be convenient on one hand since we can set
        // a specific exit code. On the other hand It's very inconvenient to have any System.exit
        // call in code that is tested.
        Thread.currentThread().setUncaughtExceptionHandler( ( t, e1 ) -> { /* Shhhh */ } );
        return launderedException( e ); // throw in order to have process exit with !0
    }

    private static void printErrorMessage( String string, Exception e, boolean stackTrace, PrintStream err )
    {
        err.println( string );
        err.println( "Caused by:" + e.getMessage() );
        if ( stackTrace )
        {
            e.printStackTrace( err );
        }
    }

    private static void printUsage( PrintStream out )
    {
        out.println( "Neo4j Import Tool" );
        for ( String line : Args.splitLongLine( "neo4j-import is used to create a new Neo4j database "
                                                + "from data in CSV files. "
                                                +
                                                "See the chapter \"Import Tool\" in the Neo4j Manual for details on the CSV file format "
                                                + "- a special kind of header is required.", 80 ) )
        {
            out.println( "\t" + line );
        }
        out.println( "Usage:" );
        for ( Options option : Options.values() )
        {
            option.printUsage( out );
        }

        out.println( "Example:");
        out.print( Strings.joinAsLines(
                TAB + "bin/neo4j-import --into retail.db --id-type string --nodes:Customer customers.csv ",
                TAB + "--nodes products.csv --nodes orders_header.csv,orders1.csv,orders2.csv ",
                TAB + "--relationships:CONTAINS order_details.csv ",
                TAB + "--relationships:ORDERED customer_orders_header.csv,orders1.csv,orders2.csv" ) );
    }

    private static boolean asksForUsage( Args args )
    {
        for ( String orphan : args.orphans() )
        {
            if ( isHelpKey( orphan ) )
            {
                return true;
            }
        }

        for ( Entry<String,String> option : args.asMap().entrySet() )
        {
            if ( isHelpKey( option.getKey() ) )
            {
                return true;
            }
        }
        return false;
    }

    private static boolean isHelpKey( String key )
    {
        return key.equals( "?" ) || key.equals( "help" );
    }
}

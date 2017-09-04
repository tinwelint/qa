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
package tooling;
/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.TimeZone;
import java.util.TreeSet;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static java.util.TimeZone.getTimeZone;

import static javax.transaction.xa.Xid.MAXBQUALSIZE;
import static javax.transaction.xa.Xid.MAXGTRIDSIZE;

import static org.neo4j.helpers.Format.DEFAULT_TIME_ZONE;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles.getLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

public class YoDump
{
    private static final String TO_FILE = "tofile";

    private final FileSystemAbstraction fileSystem;

    public YoDump( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public int dump( String filenameOrDirectory, String logPrefix, PrintStream out,
                     TimeZone timeZone ) throws IOException
    {
        int logsFound = 0;
        for ( String fileName : filenamesOf( filenameOrDirectory, logPrefix ) )
        {
            logsFound++;
            out.println( "=== " + fileName + " ===" );
            StoreChannel fileChannel = fileSystem.open( new File( fileName ), "r" );
            ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + MAXGTRIDSIZE + MAXBQUALSIZE * 10 );

            LogHeader logHeader;
            try
            {
                logHeader = readLogHeader( buffer, fileChannel, false );
            }
            catch ( IOException ex )
            {
                out.println( "Unable to read timestamp information, no records in logical log." );
                out.println( ex.getMessage() );
                fileChannel.close();
                throw ex;
            }
            out.println( "Logical log format:" + logHeader.logFormatVersion + "version: " + logHeader.logVersion +
                    " with prev committed tx[" + logHeader.lastCommittedTxId + "]" );

//            LogDeserializer deserializer = new LogDeserializer();

            PhysicalLogVersionedStoreChannel channel = new PhysicalLogVersionedStoreChannel(
                    fileChannel, logHeader.logVersion, logHeader.logFormatVersion );
            ReadableVersionableLogChannel logChannel =
                    new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, 4096 );
            Boolean state = null;
            long lastTxId = -1;
            boolean error = false;
            long lastTxIdAssigned = -1;
            long lastOwner = -1;
            try ( IOCursor<LogEntry> cursor = new LogEntryCursor( logChannel ) )
            {
                while (cursor.next())
                {
                    LogEntry entry = cursor.get();
//                    out.println( entry.toString( timeZone ) );
                    if ( entry instanceof LogEntryCommand )
                    {
                        Command command = ((LogEntryCommand) entry).getXaCommand();
                        if ( command instanceof PropertyCommand )
                        {
                            PropertyCommand propCommand = ((PropertyCommand)command);
                            Boolean stateBefore = state( propCommand.getBefore() );
                            Boolean stateAfter = state( propCommand.getAfter() );

                            if ( stateBefore != null )
                            {
                                if ( !stateBefore )
                                {
                                    throw new IllegalStateException( "Part of before state but as deleted?" );
                                }
                                if ( stateAfter == null )
                                {
                                    throw new IllegalStateException( "Part of before state but not seen in after state at all" );
                                }
                            }

                            if ( stateBefore != null || stateAfter != null )
                            {
                                Boolean newState = stateAfter;
                                if ( state != null && state.booleanValue() == newState.booleanValue() && lastOwner != propCommand.getKey() )
                                {
                                    System.out.println( "ERROR dual assignment in: " + (lastTxId+1) +
                                            " state last set in " + lastTxIdAssigned + ": " + state + " for " + command );
                                    error = true;
                                }
                                else
                                {
                                    if ( error )
                                    {
                                        System.out.println( "Reset" );
                                    }
                                    error = false;
                                }
                                // else this property was just updated, or something
                                state = newState;
                                lastTxIdAssigned = lastTxId + 1;
                                lastOwner = propCommand.getKey();
                            }
                        }
                    }
                    else if ( entry instanceof LogEntryCommit )
                    {
                        lastTxId = ((LogEntryCommit)entry).getTxId();
                    }
                }
            }
        }
        return logsFound;
    }

    private Boolean state( PropertyRecord record )
    {
        for ( DynamicRecord dynamic : record.getDeletedRecords() )
        {
            if ( dynamic.getId() == 1038971 )
            {
                return false;
            }
        }

        for ( PropertyBlock block : record )
        {
            for ( DynamicRecord dynamic : block.getValueRecords() )
            {
                if ( dynamic.getId() == 1038971 )
                {
                    return dynamic.inUse();
                }
            }
        }
        return null;
    }

    protected static boolean isAGraphDatabaseDirectory( String fileName )
    {
        File file = new File( fileName );
        return file.isDirectory() && new File( file, NeoStore.DEFAULT_NAME ).exists();
    }

    public static void main( String args[] ) throws IOException
    {
        Args arguments = Args.withFlags( TO_FILE ).parse( args );
        TimeZone timeZone = parseTimeZoneConfig( arguments );
        try ( Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                new YoDump( new DefaultFileSystemAbstraction() )
                        .dump( fileAsString, PhysicalLogFile.DEFAULT_NAME, printer.getFor( fileAsString ), timeZone );
            }
        }
    }

    public static Printer getPrinter( Args args )
    {
        boolean toFile = args.getBoolean( TO_FILE, false, true );
        return toFile ? new FilePrinter() : SYSTEM_OUT_PRINTER;
    }

    public interface Printer extends AutoCloseable
    {
        PrintStream getFor( String file ) throws FileNotFoundException;

        @Override
        void close();
    }

    private static final Printer SYSTEM_OUT_PRINTER = new Printer()
    {
        @Override
        public PrintStream getFor( String file )
        {
            return System.out;
        }

        @Override
        public void close()
        {   // Don't close System.out
        }
    };

    private static class FilePrinter implements Printer
    {
        private File directory;
        private PrintStream out;

        @Override
        public PrintStream getFor( String file ) throws FileNotFoundException
        {
            File absoluteFile = new File( file ).getAbsoluteFile();
            File dir = absoluteFile.isDirectory() ? absoluteFile : absoluteFile.getParentFile();
            if ( !dir.equals( directory ) )
            {
                safeClose();
                File dumpFile = new File( dir, "dump-logical-log.txt" );
                System.out.println( "Redirecting the output to " + dumpFile.getPath() );
                out = new PrintStream( dumpFile );
                directory = dir;
            }
            return out;
        }

        private void safeClose()
        {
            if ( out != null )
            {
                out.close();
            }
        }

        @Override
        public void close()
        {
            safeClose();
        }
    }

    public static TimeZone parseTimeZoneConfig( Args arguments )
    {
        return getTimeZone( arguments.get( "timezone", DEFAULT_TIME_ZONE.getID() ) );
    }

    protected String[] filenamesOf( String filenameOrDirectory, final String prefix )
    {

        File file = new File( filenameOrDirectory );
        if ( fileSystem.isDirectory(file) )
        {
            File[] files = fileSystem.listFiles( file , new FilenameFilter()
            {
                @Override
                public boolean accept( File dir, String name )
                {
                    return name.contains( prefix ) && !name.contains( "active" );
                }
            } );
            Collection<String> result = new TreeSet<>( sequentialComparator() );
            for ( int i = 0; i < files.length; i++ )
            {
                result.add( files[i].getPath() );
            }
            return result.toArray( new String[result.size()] );
        }
        else
        {
            return new String[] { filenameOrDirectory };
        }
    }

    private static Comparator<? super String> sequentialComparator()

    {
        return new Comparator<String>()
        {
            @Override
            public int compare( String o1, String o2 )
            {
                return versionOf( o1 ).compareTo( versionOf( o2 ) );
            }

            private Long versionOf( String string )
            {
                try
                {
                    return getLogVersion( string );
                }
                catch ( RuntimeException ignored )
                {
                    return Long.MAX_VALUE;
                }
            }
        };
    }
}

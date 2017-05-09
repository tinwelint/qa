package tooling;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.DevNullLoggingService;

public class TxLogSummary
{
    public static void main( String[] args ) throws NoSuchTransactionException, IOException
    {
        LifeSupport life = new LifeSupport();
        File storeDir = new File( args[0] );
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fs );
        TransactionIdStore txIdStore = new ReadOnlyTransactionIdStore( fs, storeDir );
        LogVersionRepository logVersionRepository = new ReadOnlyLogVersionRepository( fs, storeDir );
        TransactionMetadataCache cache = new TransactionMetadataCache( 10, 10 );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 100_000_000, txIdStore, logVersionRepository,
                new PhysicalLogFile.Monitor.Adapter(), cache ) );
        LogicalTransactionStore txStore = life.add( new PhysicalLogicalTransactionStore( logFile,
                LogRotation.NO_ROTATION, cache, txIdStore, IdOrderingQueue.BYPASS,
                new KernelHealth( new KernelPanicEventGenerator( new KernelEventHandlers( StringLogger.DEV_NULL ) ),
                        DevNullLoggingService.DEV_NULL ) ) );
        life.start();

        try ( IOCursor<CommittedTransactionRepresentation> cursor = txStore.getTransactions( 809308 ) )
        {
            while ( cursor.next() )
            {
                printSummary( cursor.get() );
            }
        }

        life.shutdown();
    }

    private static void printSummary( CommittedTransactionRepresentation committedTransactionRepresentation )
            throws IOException
    {
        TransactionRepresentation tx = committedTransactionRepresentation.getTransactionRepresentation();
        Summarizer summarizer = new Summarizer( committedTransactionRepresentation.getCommitEntry().getTxId() );
        tx.accept( summarizer );
        summarizer.print();
    }

    private static class Summarizer implements Visitor<Command,IOException>
    {
        private int relationshipCreations;
        private int addsToRelationshipTypeIndex;
        private int relationshipDeletions;
        private int removesFromRelationshipTypeIndex;

        private int relationshipTypeIndex = -1;
        private final long txId;

        public Summarizer( long txId )
        {
            this.txId = txId;
        }

        @Override
        public boolean visit( Command element ) throws IOException
        {
            if ( element instanceof RelationshipCommand )
            {
                RelationshipCommand command = (RelationshipCommand) element;
                if ( command.getRecord().inUse() && command.getRecord().isCreated() )
                {
                    relationshipCreations++;
                }
                else if ( !command.getRecord().inUse() )
                {
                    relationshipDeletions++;
                }
            }
            else if ( element instanceof IndexDefineCommand )
            {
                IndexDefineCommand command = (IndexDefineCommand) element;
                Map<String,Integer> map = command.getIndexNameIdRange();
                Integer id = map.get( "relationshipType" );
                if ( id != null )
                {
                    relationshipTypeIndex = id;
                }
            }
            else if ( element instanceof AddRelationshipCommand )
            {
                AddRelationshipCommand command = (AddRelationshipCommand) element;
                if ( command.getIndexNameId() == relationshipTypeIndex )
                {
                    addsToRelationshipTypeIndex++;
                }
            }
            else if ( element instanceof RemoveCommand )
            {
                RemoveCommand command = (RemoveCommand) element;
                if ( command.getIndexNameId() == relationshipTypeIndex )
                {
                    removesFromRelationshipTypeIndex++;
                }
            }

            return false;
        }

        private void print()
        {
            System.out.println( "Transaction " + txId );
            System.out.println( "  CREATED relationships " + relationshipCreations + ", " + addsToRelationshipTypeIndex + (relationshipCreations != addsToRelationshipTypeIndex ? " !!!!!!!" : "") );
            System.out.println( "  DELETED relationships " + relationshipDeletions + ", " + removesFromRelationshipTypeIndex + (relationshipDeletions != removesFromRelationshipTypeIndex ? " !!!!!!!" : "") );
        }
    }
}

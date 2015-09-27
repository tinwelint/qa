package tooling;

import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

public class LastCommittedTxMonitor extends Thread
{
    private volatile boolean cancel;
    private final TransactionIdStore source;

    public LastCommittedTxMonitor( TransactionIdStore source )
    {
        this.source = source;
        start();
    }

    public void cancel()
    {
        cancel = true;
    }

    @Override
    public void run()
    {
        while ( !cancel )
        {
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }

            System.out.println( source.getLastCommittedTransactionId() );
        }
    }
}

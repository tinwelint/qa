package qa;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.test.Race;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LockStarvationTest
{
    @Test
    public void shouldSeeHowLongItTakesToGetLock() throws Throwable
    {
        Locks locks =
//                new CommunityLockManger();
                new ForsetiLockManager( ResourceTypes.SCHEMA );

        Race race = new Race();
//                .withMaxDuration( 10, TimeUnit.SECONDS );
        AtomicInteger deadlocks = new AtomicInteger();
        AtomicInteger shared = new AtomicInteger();
        AtomicInteger exclusive = new AtomicInteger();
        AtomicLong waitTime = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis( 10 );

        for ( int i = 0; i < 100; i++ )
        {
            race.addContestant( () ->
            {
                while ( currentTimeMillis() < endTime )
                {
                    shared( locks, shared, deadlocks );
                }
            } );
        }
        race.addContestant( () ->
        {
            while ( currentTimeMillis() < endTime )
            {
                exclusive( locks, exclusive, waitTime );
            }
        } );

//        race.addContestants( 100, () ->
//        {
//            shared( locks, shared, deadlocks );
//        } );
//        race.addContestant( throwing( () ->
//        {
//            exclusive( locks, exclusive, waitTime );
//        } ) );
        race.go();

        System.out.println(
                " exclusive:" + exclusive +
                " shared:" + shared +
                " deadlocks:" + deadlocks +
                " wait:" + waitTime
                );
    }

    private void exclusive( Locks locks, AtomicInteger exclusive, AtomicLong waitTime )
    {
        try ( Locks.Client client = locks.newClient() )
        {
            long time = currentTimeMillis();
            client.acquireExclusive( ResourceTypes.SCHEMA, ResourceTypes.schemaResource() );
            time = currentTimeMillis() - time;
            exclusive.incrementAndGet();
            waitTime.addAndGet( time );
        }
        try
        {
            Thread.sleep( 100 );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void shared( Locks locks, AtomicInteger shared, AtomicInteger deadlocks )
    {
        try ( Locks.Client client = locks.newClient() )
        {
            client.acquireShared( ResourceTypes.SCHEMA, ResourceTypes.schemaResource() );
//                LockSupport.parkNanos( ThreadLocalRandom.current().nextInt( 50_000_000 ) );
            shared.incrementAndGet();
        }
        catch ( DeadlockDetectedException e )
        {
            deadlocks.incrementAndGet();
        }
    }
}

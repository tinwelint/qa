package qa;

import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.util.concurrent.Future;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.Locks.Client;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;

public class IndexEntryDeadlock
{
    @Rule
    public final OtherThreadRule<Void> t1 = new OtherThreadRule<>();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldLock() throws Exception
    {
        // GIVEN
        Locks locks =
                new CommunityLockManger( Config.empty(), Clock.systemUTC() );
//                new ForsetiLockManager( Config.empty(), Clock.systemUTC(), INDEX_ENTRY );
        Client c1 = locks.newClient();
        Client c2 = locks.newClient();

        // WHEN
        t1.execute( state -> {c1.acquireShared( LockTracer.NONE, INDEX_ENTRY, 0 ); return null;} ).get();
        t2.execute( state -> {c2.acquireShared( LockTracer.NONE, INDEX_ENTRY, 0 ); return null;} ).get();
        Future<Object> t1f = t1.execute( state -> {c1.acquireExclusive( LockTracer.NONE, INDEX_ENTRY, 0 ); return null;} );
        Future<Object> t2f = t2.execute( state -> {c2.acquireExclusive( LockTracer.NONE, INDEX_ENTRY, 0 ); return null;} );
        t1f.get();
        t2f.get();
        locks.close();
    }
}

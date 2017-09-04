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

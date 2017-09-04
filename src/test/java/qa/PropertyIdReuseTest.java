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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.Race;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class PropertyIdReuseTest
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void shouldNotGrowPropertyStoreIndefinitely() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();

        // WHEN
        Race race = new Race();
        final long end = currentTimeMillis() + SECONDS.toMillis( 30 );
        for ( int i = 0; i < 3; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    List<HighlyAvailableGraphDatabase> memberList = asList( cluster.getAllMembers() );
                    while ( currentTimeMillis() < end )
                    {
                        HighlyAvailableGraphDatabase db =
                                memberList.get( random.nextInt( memberList.size() ) );
//                                cluster.getMaster();
                        Node node;
                        try ( Transaction tx = db.beginTx() )
                        {
                            node = db.createNode();
                            node.setProperty( "key", stringWithLength( random.nextInt( 3, 300 ) ) );
                            tx.success();
                        }
                        try ( Transaction tx = db.beginTx() )
                        {
                            node.delete();
                            tx.success();
                        }
                    }
                }
            } );
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    while ( currentTimeMillis() < end )
                    {

                    }
                }
            } );
        }
        race.addContestant( new Runnable()
        {
            @Override
            public void run()
            {
                while ( currentTimeMillis() < end )
                {
                    try ( Transaction tx = cluster.getMaster().beginTx() )
                    {
                        try
                        {
                            Thread.sleep( 10000 );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }
                        tx.success();
                    }
                }
            }
        } );
        race.go();
    }

    protected static String stringWithLength( int length )
    {
        char[] chars = new char[length];
        Arrays.fill( chars, 'a' );
        return new String( chars );
    }
}

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.test.rule.RandomRule;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.helpers.Format.duration;

public class UnderstandWorkSyncTest
{
//    @Rule
//    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public final RandomRule random = new RandomRule();

//    @Test
//    public void shouldTest() throws Exception
//    {
//        Thread[] threads = new Thread[10];
//        for ( int i = 0; i < threads.length; i++ )
//        {
//            threads[i] = new Thread( new Runnable()
//            {
//                @Override
//                public void run()
//                {
//                    try ( Transaction tx = db.beginTx() )
//                    {
//                        for ( int j = 0; j < 5; j++ )
//                        {
//                            db.createNode( TestLabels.LABEL_ONE );
//                        }
//                        tx.success();
//                    }
//                }
//            } );
//        }
//
//        for ( int i = 0; i < threads.length; i++ )
//        {
//            threads[i].start();
//        }
//        Thread.sleep( 5000 );
//    }

    private static final int TIMES = 100_000;
    private static final int TX_SIZE = 20;
    private static final int TX_COUNT = 20;

    @Test
    public void shouldDoTheOldThing() throws Exception
    {
        long time = currentTimeMillis();
        long count = 0;
        for ( int i = 0; i < TIMES; i++ )
        {
            count += doTheOldThing();
        }
        time = currentTimeMillis() - time;
        System.out.println( duration( time ) + " " + count );
    }

    @Test
    public void shouldDoTheNewThing() throws Exception
    {
        long time = currentTimeMillis();
        long count = 0;
        for ( int i = 0; i < TIMES; i++ )
        {
            count += doTheNewThing();
        }
        time = currentTimeMillis() - time;
        System.out.println( duration( time ) + " " + count );
    }

    private static final NodeLabelUpdate END = NodeLabelUpdate.labelChanges( -1, EMPTY_LONG_ARRAY, EMPTY_LONG_ARRAY );

    private int doTheNewThing()
    {
        List<NodeLabelUpdate>[] updates = new List[TX_COUNT];
        for ( int i = 0; i < updates.length; i++ )
        {
            updates[i] = generateUpdates();
//            System.out.println( "tx" + i + ":" + LabelUpdateWork.labelUpdates( updates[i] ) );
        }
        Iterator<NodeLabelUpdate> dude = new PrefetchingIterator<NodeLabelUpdate>()
        {
            NodeLabelUpdate[] heads = new NodeLabelUpdate[updates.length];
            Iterator<NodeLabelUpdate>[] iterators = new Iterator[updates.length];

            {
                for ( int i = 0; i < updates.length; i++ )
                {
                    iterators[i] = updates[i].iterator();
                }
            }

            @Override
            protected NodeLabelUpdate fetchNextOrNull()
            {
                NodeLabelUpdate best = null;
                int bestI = 0;
                for ( int i = 0; i < updates.length; i++ )
                {
                    NodeLabelUpdate candidate = candidate( i );
                    if ( best == null || (candidate != null && candidate.getNodeId() < best.getNodeId()) )
                    {
                        best = candidate;
                        bestI = i;
                    }
                }
                heads[bestI] = null;
                return best;
            }

            private NodeLabelUpdate candidate( int i )
            {
                if ( heads[i] == END )
                {
                    return null;
                }

                if ( heads[i] == null )
                {
                    if ( iterators[i].hasNext() )
                    {
                        heads[i] = iterators[i].next();
                    }
                    else
                    {
                        heads[i] = END;
                        return null;
                    }
                }
                return heads[i];
            }
        };
        int count = 0;
        while ( dude.hasNext() )
        {
            NodeLabelUpdate man = dude.next();
            if ( man != null )
            {
//                System.out.println( man.getNodeId() );
                count++;
            }
        }
        return count;
    }

    private int doTheOldThing()
    {
        List<NodeLabelUpdate> updates = new ArrayList<>();
        for ( int i = 0; i < TX_COUNT; i++ )
        {
            List<NodeLabelUpdate> set = generateUpdates();
            updates.addAll( set );
        }
        updates.sort( NodeLabelUpdate.SORT_BY_NODE_ID );
        int count = 0;
        for ( NodeLabelUpdate nodeLabelUpdate : updates )
        {
            if ( nodeLabelUpdate != null )
            {
                count++;
            }
        }
        return count;
    }

    private List<NodeLabelUpdate> generateUpdates()
    {
        List<NodeLabelUpdate> list = new ArrayList<>();
        long id = random.nextLong( 100_000 );
        for ( int i = 0; i < TX_SIZE; i++ )
        {
            list.add( NodeLabelUpdate.labelChanges( id, EMPTY_LONG_ARRAY, EMPTY_LONG_ARRAY ) );
            id += random.nextInt( 1_000 ) + 1;
        }
        return list;
    }
}

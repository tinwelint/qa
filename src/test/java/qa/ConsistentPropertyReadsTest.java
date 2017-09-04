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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.Race;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsistentPropertyReadsTest
{
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

    @Test
    public void shouldReadConsistentPropertyValues() throws Throwable
    {
        // GIVEN
        Node[] nodes = new Node[10];
        final String[] keys = new String[] {"1", "2", "3"};
        final String[] values = new String[] {
                "dsjlfhsdljhsjlghsljfghjlsfhg jlfh jldfgh djlfghdljfgh dljfghdfljgh dlfjg hdfljg hdfljg hdflghdfjgl hdfljg hdfjlg hdfglj hdfgjl dhfglj dhfgjl dfhglsdjfköasjkdlöadksflösf akljdhfwjl htlj3ht jl3rht jl3ht j3lht 3jt h3ltj h34tl j3ht j3t h3jtl h34jtl 3h4tl 3j4ht l34jt h3ljg",
                "jdlhflahsjlw4hjltwrhtjl4hljterhglwrhyjl5hgwjlrgh rljeghlj rhyljthlj5 4yhj tlrwhtlj rhylj hrjtl h53jl6ht 35qjlhjl4 htjl rh jtl35 hyjl43 hyjl35hjylhwjelth tq3jlth qjlrwhetj lq3rht lqrjwhqlj rhyjl q3htjlhr tjlwhrg jlwrhy jlrht jlgwhjly h3rjl t h",
                "kfgjlyh3jlghjl45h l4tg hj45l h46jl ghjlgh5glj  hgjl 5hgjl5hjl gh4j3 lgh46 ljh4 jl4h j4hg4lgj h4t jlgh 4t jlgh4tgjlthgje tlhjfldhb8ther8g0u8f0vrehi5köehvjlfhgutwohy ireghlwgh lrej htlweghwrufh wrutl h35ut lhrgvj rhyl5hy jlwhglwrhfu5oy h53tlj h4wlt h5eut herul"
        };
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                nodes[i] = db.createNode();
                for ( int j = 0; j < keys.length; j++ )
                {
                    nodes[i].setProperty( keys[j], values[0] );
                }
            }
            tx.success();
        }

        int updaters = 10;
        AtomicLong updatersDone = new AtomicLong( updaters );
        Race race = new Race();
        for ( int i = 0; i < updaters; i++ )
        {
            // Changers
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        ThreadLocalRandom random = ThreadLocalRandom.current();
                        for ( int j = 0; j < 100; j++ )
                        {
                            try ( Transaction tx = db.beginTx() )
                            {
                                nodes[random.nextInt( nodes.length )].setProperty( keys[random.nextInt( keys.length )],
                                        values[random.nextInt( values.length )] );
                                tx.success();
                            }
                        }
                    }
                    finally
                    {
                        updatersDone.decrementAndGet();
                    }
                }
            } );
        }
        for ( int i = 0; i < 10; i++ )
        {
            // Readers
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while ( updatersDone.get() > 0 )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            String value = (String) nodes[random.nextInt( nodes.length )]
                                    .getProperty( keys[random.nextInt( keys.length )] );
                            assertTrue( value, ArrayUtil.contains( values, value ) );
                            tx.success();
                        }
                    }
                }
            } );
        }

        // WHEN
        race.go();
    }

    @Ignore( "We make no guarantee about this, but interesting to have as perspective" )
    @Test
    public void shouldReadPropertiesFromOneEntityConsistently() throws Throwable
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "key1", "one" );
            node.setProperty( "key2", 1 );
            tx.success();
        }

        final AtomicBoolean end = new AtomicBoolean();
        Race race = new Race();
        race.addContestant( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.setProperty( "key1", "two" );
                    node.setProperty( "key2", 2 );
                    tx.success();
                }
                end.set( true );
            }
        } );
        for ( int i = 0; i < 10; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    int afterwards = 10;
                    while ( !end.get() || --afterwards > 0)
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            Object value1 = node.getProperty( "key1" );
                            Object value2 = node.getProperty( "key2" );

                            if ( value1.equals( "one" ) )
                            {
                                assertEquals( 1, value2 );
                            }
                            else if ( value1.equals( "two" ) )
                            {
                                assertEquals( 2, value2 );
                            }
                            else
                            {
                                fail( "Weird state as a whole: " + value1 + " and " + value2 );
                            }
                            tx.success();
                        }
                    }
                }
            } );
        }
        race.go();
    }

    @Ignore( "We make no guarantee about this, but interesting to have as perspective" )
    @Test
    public void shouldReadPropertiesFromOneEntityConsistentlyUsingGetAllProperties() throws Throwable
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "key1", "one" );
            node.setProperty( "key2", 1 );
            tx.success();
        }

        final AtomicBoolean end = new AtomicBoolean();
        Race race = new Race();
        race.addContestant( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.setProperty( "key1", "two" );
                    node.setProperty( "key2", 2 );
                    tx.success();
                }
                end.set( true );
            }
        } );
        for ( int i = 0; i < 10; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    int afterwards = 10;
                    while ( !end.get() || --afterwards > 0)
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            Map<String,Object> values = node.getProperties( "key1", "key2" );
                            Object value1 = values.get( "key1" );
                            Object value2 = values.get( "key2" );

                            if ( value1.equals( "one" ) )
                            {
                                assertEquals( 1, value2 );
                            }
                            else if ( value1.equals( "two" ) )
                            {
                                assertEquals( 2, value2 );
                            }
                            else
                            {
                                fail( "Weird state as a whole: " + value1 + " and " + value2 );
                            }
                            tx.success();
                        }
                    }
                }
            } );
        }
        race.go();
    }
}

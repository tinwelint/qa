/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

public class IsoTest
{
    public static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    private static final long TIME = TimeUnit.SECONDS.toMillis( 10 );

    private static class Stats
    {
        int successfulRead, unexpectedRead, failureRead, failureWrite;

        void accumulateFrom( Stats other )
        {
            successfulRead += other.successfulRead;
            unexpectedRead += other.unexpectedRead;
            failureRead += other.failureRead;
            failureWrite += other.failureWrite;
        }

        @Override
        public String toString()
        {
            return format( "successfulRead:%d%nunexpectedRead:%d%nfailureRead:%d%nfailureWrite:%d",
                    successfulRead, unexpectedRead, failureRead, failureWrite );
        }
    }

    public static void main( String[] args ) throws IOException
    {
        File storeDir = new File( "isotest.db" );
        System.out.println( storeDir.getCanonicalPath() );

        FileUtils.deleteRecursively( storeDir );
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir.getPath() )
                .newGraphDatabase();

        ExecutorService executorService = Executors.newFixedThreadPool( MAX_THREADS );

        ArrayList<ChainMangler> manglers = new ArrayList<>();

        for ( int unique = 0; unique < MAX_THREADS*3; unique++ )
        {
            manglers.add ( new ChainMangler( graphDb, unique, TIME, ChainMangler.Op.ADD_REMOVE ) );
            manglers.add ( new ChainMangler( graphDb, unique, TIME, ChainMangler.Op.CHECK ) );
        }

        try
        {
            for ( Future<Void> iterations : executorService.invokeAll( manglers ) )
            {
                iterations.get();
            }
        }
        catch ( InterruptedException | ExecutionException e )
        {
            e.printStackTrace();
        }

        Stats stats = new Stats();
        for ( ChainMangler mangler : manglers )
        {
            stats.accumulateFrom( mangler.stats() );
        }
        System.out.println( stats );

        executorService.shutdown();
        graphDb.shutdown();

        System.out.println("Shutdown.");
    }

    static class ChainMangler implements Callable<Void>
    {
        enum Op
        {
            ADD_REMOVE,
            CHECK;
        }

        private static final int LEN = 25;

        private final GraphDatabaseService graphDb;
        private final long time;
        private final Op op;
        private final String unique;
        private final Stats stats = new Stats();

        private long endTime;

        public ChainMangler( GraphDatabaseService graphDb, int unique, long time, Op op )
        {
            this.graphDb = graphDb;
            this.time = time;
            this.op = op;
            this.unique = strOfLen( LEN, unique );
        }

        private String strOfLen( int len, int unique )
        {
            String s = "";
            for ( int i = 0; i < len; i++ )
            {
                s = s + unique;
            }
            return s;
        }

        @Override
        public Void call() throws Exception
        {
            this.endTime = currentTimeMillis() + time;
            switch ( op )
            {
            case ADD_REMOVE:
                addRemove();
                break;
            case CHECK:
                check();
                break;
            default:
                throw new IllegalArgumentException();
            }
            return null;
        }

        private void check() throws InterruptedException
        {
            Node myNode;

            Thread.sleep( 1000 );

            try ( Transaction tx = graphDb.beginTx() )
            {
                myNode = graphDb.findNodes( DynamicLabel.label( unique ) ).next();
                tx.success();
            }

            while ( currentTimeMillis() < endTime )
            {
                try
                {
                    try ( Transaction tx = graphDb.beginTx() )
                    {
//                        Iterable<String> propertyKeys = myNode.getPropertyKeys();
//                        for ( String propertyKey : propertyKeys )
                        for ( Map.Entry<String,Object> property : myNode.getAllProperties().entrySet() )
                        {
                            Object propertyValue =
                                    property.getValue();
//                                    myNode.getProperty( propertyKey );
                            if ( !propertyValue.equals( unique ) )
                            {
                                System.out.println( "Expected:" + unique + " Got:" + propertyValue );
                                stats.unexpectedRead++;
                            }
                            else
                            {
                                stats.successfulRead++;
                            }
                        }
                        tx.success();
                    }
                }
                catch ( NotFoundException e )
                {
                    //e.printStackTrace();
//                    System.out.println( String.format( "CHECK not found exception after: %d", iteration ) );
                    stats.failureRead++;
                }
            }

            System.out.println("Check done");
        }

        private void addRemove()
        {
            Node myNode;

            try ( Transaction tx = graphDb.beginTx() )
            {
                myNode = graphDb.createNode( DynamicLabel.label( unique ) );
                tx.success();
            }

            while ( currentTimeMillis() < endTime )
            {
                try
                {
                    try ( Transaction tx = graphDb.beginTx() )
                    {
                        myNode.setProperty( "key1", unique );
                        myNode.setProperty( "key2", unique );
                        myNode.setProperty( "key3", unique );
                        tx.success();
                    }

                    try ( Transaction tx = graphDb.beginTx() )
                    {
                        myNode.removeProperty( "key2" );
                        myNode.removeProperty( "key1" );
                        myNode.removeProperty( "key3" );
                        tx.success();
                    }
                }
                catch ( NotFoundException e )
                {
                    e.printStackTrace();
                    //System.out.println( String.format( "Not found exception after: %d", iteration ) );
                    stats.failureWrite++;
                }
            }
        }

        synchronized Stats stats()
        {
            return stats;
        }
    }
}

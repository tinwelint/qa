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

import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;

public class RecoverOldStuff
{
    private static File storeDir = new File( "target/" + RecoverOldStuff.class.getSimpleName() );

    @Test
    public void stepOne() throws Exception
    {
        FileUtils.deleteRecursively( storeDir );
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.check_point_interval_time, "500ms" )
                .newGraphDatabase();

        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }

        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                node.setProperty( "key", i );
                tx.success();
            }
            Thread.sleep( 50 );
        }
        db.shutdown();
    }

    @Test
    public void stepTwo() throws Exception
    {
        File file = new File( storeDir, PhysicalLogFile.DEFAULT_NAME + ".0" );
        try ( RandomAccessFile raFile = new RandomAccessFile( file, "rw" );
              FileChannel channel = raFile.getChannel() )
        {
            channel.write( aSingleZero(), 3085 );
        }
    }

    @Test
    public void stepThree() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try ( Transaction tx = db.beginTx() )
        {
            System.out.println( db.getNodeById( 0 ).getProperty( "key" ) );
            tx.success();
        }
        db.shutdown();
    }

    private static ByteBuffer aSingleZero()
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1 );
        buffer.put( (byte) 67 );
        buffer.flip();
        return buffer;
    }
}

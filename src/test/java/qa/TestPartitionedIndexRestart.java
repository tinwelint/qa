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

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestPartitionedIndexRestart
{
    private static final int partitionSize = Integer.getInteger( "luceneSchemaIndex.maxPartitionSize" );
    private static final String key = "key";
    private static final Label label = Label.label( "Lbl" );

    public static void main( String[] args ) throws IOException
    {
        File storeDir = new File( "part-index" );
//        FileUtils.deleteRecursively( storeDir );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
//            for ( int t = 0; t < 5; t++ )
//            {
//                try ( Transaction tx = db.beginTx() )
//                {
//                    for ( int i = 0; i < partitionSize; i++ )
//                    {
//                        db.createNode( label ).setProperty( key, i );
//                    }
//                    tx.success();
//                }
//            }
//            createIndex( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void createIndex( GraphDatabaseService db )
    {
        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.schema().indexFor( label ).on( key ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 1, MINUTES );
            tx.success();
        }
        System.out.println( "ONLINE" );
    }
}

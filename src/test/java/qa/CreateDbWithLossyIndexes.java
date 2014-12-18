/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

import static org.neo4j.graphdb.DynamicLabel.label;

public class CreateDbWithLossyIndexes
{
    private static final Label labelOne = label( "One" ), labelTwo = label( "Two" ), labelThree = label( "Three" );
    private static final String keyOne = "name", keyTwo = "numbers", keyThree = "mixed";
    private static final long lossyLongValueOne = 2147483647000577536L, lossyLongValueTwo = 2147483647000577465L;
    private static final double
            lossyDoubleValueOne = 10000000000000000000.0D,
            lossyDoubleValueTwo = 9223372036854775807.0D;

    @Test
    public void createLegacyDb() throws Exception
    {
        // GIVEN
        String path = "lossy-index-db";
        FileUtils.deleteRecursively( new File( path ) );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        try
        {
            createIndex( db, labelOne, keyOne );
            createIndex( db, labelTwo, keyTwo );
            createIndex( db, labelThree, keyThree );
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
                tx.success();
            }

            createNodes( db, labelOne, keyOne, "Ben", "Alistair", "Johan" );
            createNodes( db, labelTwo, keyTwo, 1L, 10324L, lossyLongValueOne, lossyLongValueTwo,
                    lossyDoubleValueOne, lossyDoubleValueTwo );
            createNodes( db, labelThree, keyThree, "Mattias", "Johan", 10324L, lossyLongValueOne, lossyLongValueTwo );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void createNodes( GraphDatabaseService db, Label label, String propertyKey, Object... propertyValues )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Object propertyValue : propertyValues )
            {
                db.createNode( label ).setProperty( propertyKey, propertyValue );
            }
            tx.success();
        }
    }

    private void createIndex( GraphDatabaseService db, Label label, String string )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( string ).create();
            tx.success();
        }
    }

//    public static void main( String[] args )
//    {
//        System.out.println( lossyDoubleValueOne );
//        System.out.println( lossyDoubleValueTwo );
//
//        System.out.println( doubleValueCanCoerceCleanlyIntoLong( lossyDoubleValueOne ) );
//        System.out.println( doubleValueCanCoerceCleanlyIntoLong( lossyDoubleValueTwo ) );
//    }
//
//    public static boolean doubleValueCanCoerceCleanlyIntoLong( double doubleValue )
//    {
//        long intermediaryLong = (long) doubleValue;
//        double doubleAfterRoundTrip = intermediaryLong;
//        return doubleValue == doubleAfterRoundTrip;
//    }
}

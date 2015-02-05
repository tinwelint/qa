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

import org.junit.Test;

import java.io.File;
import java.util.Random;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class CreateBigDb
{
    @Test
    public void should() throws Exception
    {
        // GIVEN
        String storeDir = "big-db-1M";
        FileUtils.deleteRecursively( new File( storeDir ) );
        BatchInserter inserter = BatchInserters.inserter( storeDir );

        // WHEN
        int maxNodes = 1_000_000;
        for ( int i = 0; i < maxNodes; i++ )
        {
            inserter.createNode( null );
            if ( i % 1_000_000 == 0 )
            {
                System.out.println( "node:" + i );
            }
        }
        Random random = new Random();
        for ( int i = 0; i < maxNodes; i++ )
        {
            long startNode = random.nextInt( maxNodes );
            int rels = random.nextInt( 20 )+5;
            for ( int j = 0; j < rels; j++ )
            {
                inserter.createRelationship( startNode, random.nextInt( maxNodes ),
                        MyRelTypes.values()[j%MyRelTypes.values().length], null );
            }
            if ( i % 100_000 == 0 )
            {
                System.out.println( "rel group:" + i );
            }
        }

        // THEN
        inserter.shutdown();
    }
}

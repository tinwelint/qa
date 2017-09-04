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

import org.junit.Test;
import versiondiff.VersionDifferences;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class CreateDbWithDataAndCheckPoints
{
    @Test
    public void should() throws Exception
    {
        GraphDatabaseService db = VersionDifferences.newDb( clean( "checkpoint-db" ) );
        try
        {
            createNode( db, 0 );
            checkPoint( db );
            createNode( db, 1 );
            checkPoint( db );
            checkPoint( db );
            createNode( db, 2 );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void checkPoint( GraphDatabaseService db ) throws IllegalArgumentException, IOException
    {
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( CheckPointer.class )
                .forceCheckPoint( new SimpleTriggerInfo( "bla" ) );
    }

    private void createNode( GraphDatabaseService db, int value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "key", value );
            tx.success();
        }
    }

    private String clean( String file ) throws IOException
    {
        FileUtils.deleteRecursively( new File( file ) );
        return file;
    }
}

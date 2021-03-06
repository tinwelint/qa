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

import com.google.common.jimfs.Jimfs;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DelegateFileSystemAbstraction;
import org.neo4j.test.TestGraphDatabaseFactory;

public class ImpermanentGraphDatabaseWithJimFs
{
    @Test
    public void shouldStartAnImpermanentGraphDatabaseUsingJimFS() throws Exception
    {
        // GIVEN
        GraphDatabaseService inMemoryDb = new TestGraphDatabaseFactory()
                .setFileSystem( new DelegateFileSystemAbstraction( Jimfs.newFileSystem() ) )
                .newImpermanentDatabase();

        try ( Transaction tx = inMemoryDb.beginTx() )
        {
            // WHEN
            inMemoryDb.createNode();
            tx.success();
        }

        // THEN
        inMemoryDb.shutdown();
    }
}

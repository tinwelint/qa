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
package tooling;

import versiondiff.VersionDifferences;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class StartDbWithBackupEnabled
{
    public static void main( String[] args ) throws Exception
    {
        GraphDatabaseService db = VersionDifferences.newDb( args[0] );

//        Future<?> load = new SillyLoad( db, 10, 1000 );

        System.out.println( "Db started, with some load going into it. ENTER to quit" );
        System.in.read();
//        load.cancel( false );
//        load.get();
        db.shutdown();
    }
}

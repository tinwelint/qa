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
import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class ManualFormatFamilyMigrationTest
{
//    @Test
//    public void shouldNotBeAbleToMigrateBackwardsForSameCapabilityFormats() throws Exception
//    {
//        // GIVEN
//
//        // WHEN
//        // THEN
//        fail( "Test not fully implemented yet" );
//    }

    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( new File( "migration" ) )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" )
                .setConfig( GraphDatabaseSettings.record_format, "standard" )
//                .setConfig( GraphDatabaseSettings.record_format, "high_limit" )
                .newGraphDatabase();
        db.shutdown();
    }
}

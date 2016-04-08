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
package tooling;

import java.io.File;

import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class UpgradeDb
{
    public static void main( String[] args )
    {
        new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( new File( args[0] ) )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" )
//                .setConfig( GraphDatabaseFacadeFactory.Configuration.record_format, LowLimitV3_0.NAME )
                .newGraphDatabase()
                .shutdown();
    }
}

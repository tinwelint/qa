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
package qa;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;

import static java.lang.System.currentTimeMillis;

public class UpgradeDb
{
    @Test
    public void shouldUpgradeDb() throws Exception
    {
        // GIVEN
        long time = currentTimeMillis();

        // WHEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( "empty-db" )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade.name(), "true" )
                .newGraphDatabase();
        time = currentTimeMillis()-time;

        // THEN
//        printDenseNodeCount( db );
        System.out.println( time );

        db.shutdown();
    }

//    private void printDenseNodeCount( GraphDatabaseAPI db )
//    {
//        NeoStore neoStore = db.getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate();
//        NodeStore nodeStore = neoStore.getNodeStore();
//        long high = nodeStore.getHighestPossibleIdInUse();
//        int dense = 0;
//        for ( long i = 0; i < high; i++ )
//        {
//            NodeRecord record = nodeStore.forceGetRaw( i );
//            if ( record.isDense() )
//            {
//                dense++;
//            }
//        }
//        System.out.println( "dense count:" + dense );
//    }
}

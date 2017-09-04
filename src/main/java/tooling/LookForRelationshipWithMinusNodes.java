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

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.impl.store.RelationshipStore;

public class LookForRelationshipWithMinusNodes
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( new File( args[0] ) );
        try
        {
            RelationshipStore store = VersionDifferences.neoStores( db ).getRelationshipStore();
            VersionDifferences.scanAllRecords( store, relationship ->
            {
                if ( relationship.getFirstNode() == -1 || relationship.getSecondNode() == -1 )
                {
                    System.out.println( "Yup " + relationship );
                }
                return false;
            } );
        }
        finally
        {
            db.shutdown();
        }
    }
}

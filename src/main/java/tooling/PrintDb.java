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

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class PrintDb
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = VersionDifferences.newDb( args[0] );
        try ( Transaction tx = db.beginTx() )
        {
            int nodes = 0;
            for ( Node node : db.getAllNodes() )
            {
                print( node );
                for ( Relationship relationship : node.getRelationships() )
                {
                    print( relationship );
                }

                if ( nodes++ >= 100 )
                {
                    System.out.println( "... breaking ..." );
                    break;
                }
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void print( Relationship relationship )
    {
        System.out.println( relationship );
        printProperties( relationship );
    }

    private static void print( Node node )
    {
        System.out.println( node );
        printProperties( node );
    }

    private static void printProperties( PropertyContainer entity )
    {
        for ( Map.Entry<String,Object> property : entity.getAllProperties().entrySet() )
        {
            System.out.println( "  " + property );
        }
    }
}

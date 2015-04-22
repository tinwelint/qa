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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.tooling.GlobalGraphOperations;

public class GoThroughStore
{
    private static final Comparator<Relationship> RELATIONSHIPS_SORTER = new Comparator<Relationship>()
    {
        @Override
        public int compare( Relationship o1, Relationship o2 )
        {
            Long i1 = o1.getId();
            Long i2 = o2.getId();
            return i1.compareTo( i2 );
        }
    };

    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( args[0] )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE )
                .newGraphDatabase();
        Transaction tx = db.beginTx();
        try
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                System.out.println( "Node " + node.getId() + " : " + propertiesOf( node ) );
                List<Relationship> relationships = new ArrayList<>();
                for ( Relationship rel : node.getRelationships( Direction.INCOMING ) )
                {
                    relationships.add( rel );
                }
                Collections.sort( relationships, RELATIONSHIPS_SORTER );

                for ( Relationship rel : relationships )
                {
                    System.out.println( "  " + rel.getId() + "(" + rel.getType().name() + ") : " + propertiesOf( rel ) );
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
            db.shutdown();
        }
    }

    private static Map<String, Object> propertiesOf( PropertyContainer entity )
    {
        Map<String, Object> properties = new HashMap<>();
        for ( String key : entity.getPropertyKeys() )
        {
            properties.put( key, entity.getProperty( key ) );
        }
        return properties;
    }
}

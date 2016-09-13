/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;

import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class IdReuseIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( EnterpriseEditionSettings.id_reuse_safe_zone_time, "1m" )
            .withSharedSetting( EnterpriseEditionSettings.idTypesToReuse, "RELATIONSHIP" );

    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();

        // WHEN
        List<HighlyAvailableGraphDatabase> dbs = asList( cluster.getAllMembers() );
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while ( true )
        {
            Thread.sleep( 5000 );
            HighlyAvailableGraphDatabase db = dbs.get( random.nextInt( dbs.size() ) );
            try ( Transaction tx = db.beginTx() )
            {
                Node node = randomNode( db, random );
                if ( node == null || random.nextFloat() > 0.3 )
                {
                    // Create 2/3
                    createTree( db );
                }
                else
                {
                    // Delete 1/3
                    deleteTree( node );
                }
                tx.success();
            }
        }
    }

    private void deleteTree( Node node )
    {
        for ( Relationship relationship : node.getRelationships() )
        {
            relationship.delete();
            System.out.println( "Deleting rel " + relationship.getId() );
        }
        node.delete();
    }

    private void createTree( HighlyAvailableGraphDatabase db )
    {
        Node node = db.createNode();
        for ( int i = 0; i < 3; i++ )
        {
            long rel = node.createRelationshipTo( node, MyRelTypes.TEST ).getId();
            System.out.println( "Creating rel " + rel );
        }
    }

    private Node randomNode( HighlyAvailableGraphDatabase db, ThreadLocalRandom random )
    {
        List<Node> allNodes = asList( db.getAllNodes() );
        return allNodes.isEmpty() ? null : allNodes.get( random.nextInt( allNodes.size() ) );
    }
}

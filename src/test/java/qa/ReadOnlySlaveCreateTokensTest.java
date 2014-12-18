/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.test.TestLabels;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadOnlySlaveCreateTokensTest extends AbstractClusterTest
{
    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
        if ( serverId.toIntegerIndex() > 1 )
        {
            builder.setConfig( GraphDatabaseSettings.read_only, Settings.TRUE );
        }
    }

    @Test
    public void shouldNotAllowReadOnlySlaveCreatingTokens() throws Exception
    {
        cluster.await( ClusterManager.allSeesAllAsAvailable() );
        GraphDatabaseService slave = cluster.getAnySlave();
        Transaction tx = slave.beginTx();
        try
        {
            // The node created here will fail to commit
            // However the label token will be created on the master, which is not read-only
            slave.createNode( TestLabels.LABEL_ONE );
            tx.success();
            tx.close();
            fail( "Should not allow to commit" );
        }
        catch ( TransactionFailureException e )
        {
            e.printStackTrace();
            assertTrue( Exceptions.contains( e, ReadOnlyDbException.class ) );
        }
        GraphDatabaseAPI master = cluster.getMaster();
        int id = master.getDependencyResolver().resolveDependency( LabelTokenHolder.class )
                .getIdByName( TestLabels.LABEL_ONE.name() );
        assertEquals( -1, id );
    }
}

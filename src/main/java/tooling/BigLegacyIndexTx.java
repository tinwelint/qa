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

import tooling.CommandReactor.Action;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Args;

import static java.lang.Boolean.parseBoolean;

class BigLegacyIndexTx implements Action
{
    private final GraphDatabaseService db;

    public BigLegacyIndexTx( GraphDatabaseService db )
    {
        this.db = db;
    }

    @Override
    public void run( Args action ) throws Exception
    {
        int size = Integer.parseInt( action.orphans().get( 1 ) );
        boolean updateLegacyIndex = true;
        if ( action.orphans().size() > 2 && !parseBoolean( action.orphans().get( 2 ) ) )
        {
            updateLegacyIndex = false;
        }
        System.out.println( updateLegacyIndex );
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( "indexo" );
            for ( int i = 0; i < size; i++ )
            {
                Node node = db.createNode();
                String key = "key" + i;
                Object value = i;
                if ( updateLegacyIndex )
                {
                    index.add( node, key, value );
                }
                node.setProperty( key, value );
            }
            tx.success();
        }
    }
}

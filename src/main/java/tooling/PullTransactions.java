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

import org.neo4j.helpers.Args;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.UpdatePuller;

public class PullTransactions implements Action
{
    private final GraphDatabaseAPI db;
    private final UpdatePuller puller;

    public PullTransactions( GraphDatabaseAPI db )
    {
        this.db = db;
        this.puller = db.getDependencyResolver().resolveDependency( UpdatePuller.class );
    }

    @Override
    public void run( Args action ) throws Exception
    {
        puller.pullUpdates();
    }
}

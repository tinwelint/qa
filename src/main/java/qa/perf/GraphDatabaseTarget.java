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
package qa.perf;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;

public class GraphDatabaseTarget implements Target
{
    public GraphDatabaseService db;
    private final Map<String,String> config;
    private File storeDir;
    public Object state;

    public GraphDatabaseTarget( String... config )
    {
        this.config = MapUtil.stringMap( config );
    }

    public GraphDatabaseTarget onExistingStoreDir( File storeDir )
    {
        this.storeDir = storeDir;
        return this;
    }

    @Override
    public void start() throws IOException
    {
        if ( storeDir == null )
        {
            storeDir = new File( DEFAULT_DIR );
            FileUtils.deleteRecursively( storeDir );
        }

        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( config )
                .newGraphDatabase();
    }

    GraphDatabaseService db()
    {
        return db;
    }

    @Override
    public void stop()
    {
        db.shutdown();
    }
}

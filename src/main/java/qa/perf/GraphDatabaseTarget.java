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

    public GraphDatabaseTarget( String... config )
    {
        this.config = MapUtil.stringMap( config );
    }

    @Override
    public void start() throws IOException
    {
        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( clear( "target/test-data/performance-tests" ) )
                .setConfig( config )
                .newGraphDatabase();
    }

    private String clear( String string ) throws IOException
    {
        FileUtils.deleteRecursively( new File( string ) );
        return string;
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

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
package qa;

import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CreateIndexTest
{
//    @Rule
//    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() )
//    {
//        @Override
//        protected void configure( org.neo4j.graphdb.factory.GraphDatabaseBuilder builder )
//        {
//            builder.setConfig( GraphDatabaseFacadeFactory.Configuration.lock_manager, "community" );
//        }
//    };

    @Test
    public void shouldCreateIndex() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( "yoyo" ) );
        if (
                true )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( Label.label( "kjdfk" ) ).assertPropertyIsUnique( "yo" ).create();
                tx.success();
            }
        }
        db.shutdown();
    }
}

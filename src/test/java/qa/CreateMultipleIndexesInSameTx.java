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

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;

import static org.neo4j.graphdb.DynamicLabel.label;

public class CreateMultipleIndexesInSameTx
{
    @Test
    public void shouldCreateMultipleIndexes() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "multi-index" );
        Label label = label( "Labello" );

        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label ).on( "one" ).create();
                db.schema().indexFor( label ).on( "two" ).create();
                db.schema().indexFor( label ).on( "three" ).create();
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( IndexDefinition index : db.schema().getIndexes() )
                {
                    System.out.println( index );
                }
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }
}

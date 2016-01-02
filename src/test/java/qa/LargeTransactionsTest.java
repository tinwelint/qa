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

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static qa.perf.Operations.single;
import static qa.perf.Performance.measure;

public class LargeTransactionsTest
{
    private static final Label[] labels = new Label[] { DynamicLabel.label( "L1" ), DynamicLabel.label( "L2" ) };

    @Test
    public void shouldMeasure() throws Exception
    {
        // GIVEN
        GraphDatabaseTarget target = new GraphDatabaseTarget();
        Operation<GraphDatabaseTarget> initial = new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    for ( int i = 0; i < labels.length; i++ )
                    {
                        on.db.schema().indexFor( labels[i] ).on( "string1" ).create();
                        on.db.schema().indexFor( labels[i] ).on( "string2" ).create();
                        on.db.schema().indexFor( labels[i] ).on( "long1" ).create();
                    }
                    tx.success();
                }
                try ( Transaction tx = on.db.beginTx() )
                {
                    on.db.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
                    tx.success();
                }
            }
        };
        Operation<GraphDatabaseTarget> operation = new Operation<GraphDatabaseTarget>()
        {
            private int totali;

            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    for ( int i = 0; i < 50_000; i++, totali++ )
                    {
                        Node node = on.db.createNode( labels );
                        node.setProperty( "long1", 492853485934859L * totali );
                        node.setProperty( "long2", 457847583L * i );
                        node.setProperty( "string1", "kdjffkghljdfhglfd hgdfjsghdlfgj hdfjlgh dlfgh jlfbh " + totali );
                        node.setProperty( "string2", "wi0587dv80b78d0fg78s07gdf07g80sc7b0s8fg6s9d76g79scv67dsf96v97xf6 ds96d 79sv6s9d f6v7 ds6vs7aaksldjlasjd lsjdkdjfl aslkd js9 " + i );
                        node.setProperty( "string3", "wi0587dv80b78d0fg78s07gdf07g80sc7b0s8fg6s9d76g79scv67dsf96v97xf6 ds96d 79sv6s9d f6v7 ds6vs79 " + totali );
                        node.setProperty( "string4", "wi0587dv80b78d0fg78s07gdf07g80sc7b0s8fg6s9d76g79scv67dsf96v97xf6 ds96d 79sv6s9d f6v7 ds6vs79asjlch adjch jlsdhf adljsghlsadhf ljsahf vjsafgh ljsahvjlsfhgjladhfl jdhfjl ahdfljadhfjl sahdflj ahdf j" + i );
                    }
                    tx.success();
                }
            }
        };

        // THEN
        measure( target, initial, single( operation ), 1, 120 );
    }
}

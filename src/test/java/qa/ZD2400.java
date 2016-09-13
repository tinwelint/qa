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
package qa;

import org.junit.Test;
import qa.perf.GraphDatabaseTarget;
import qa.perf.Operation;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static qa.perf.Operations.single;
import static qa.perf.Performance.measure;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ZD2400
{
    private static final Label GOOD_READS = label( "GoodReads" );
    private static final String ID = "goodreadsId";

    @Test
    public void shouldReproduce() throws Exception
    {
        // WHEN
        measure( new GraphDatabaseTarget(), initialData(), single( query() ), 1, 0, 0 );

        // THEN don't throw exception
    }

    private Operation<GraphDatabaseTarget> initialData()
    {
        return new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                try ( Transaction tx = on.db.beginTx() )
                {
                    on.db.schema().constraintFor( GOOD_READS ).assertPropertyIsUnique( ID ).create();
                    tx.success();
                }

                for ( int i = 0; i < 1000; i++ )
                {

                }
            }
        };
    }

    private Operation<GraphDatabaseTarget> query()
    {
        return new Operation<GraphDatabaseTarget>()
        {
            @Override
            public void perform( GraphDatabaseTarget on )
            {
                // param1: "25979_book"    (:GoodReads node with this "goodreadsId" property)
                // param2: "26690_edition" (:GoodReads node with this "goodreadsId" property)
                // param3: ["847_author"]  (:GoodReads node with this "goodreadsId" property)
                // param4: "26690"         (:GoodReads node with this "goodreadsId" property)
                // param5: "The Draft"     (:GoodReads node "title" property)
                // param6: "2006"          (:GoodReads node "releaseDate" property)
                // param7: "GoodReads"     (:GoodReads node "source" property)
                on.db.execute( "MATCH (sbf:GoodReads) USING INDEX sbf:GoodReads(goodreadsId) WHERE sbf.goodreadsId = {param1} WITH sbf MATCH (sa:GoodReads) USING INDEX sa:GoodReads(goodreadsId) WHERE sa.goodreadsId = {param2} WITH sbf, sa MATCH (sc:GoodReads) USING INDEX sc:GoodReads(goodreadsId) WHERE sc.goodreadsId IN {param3} WITH DISTINCT sbf, sa, collect(DISTINCT sc) AS scs MERGE (sb:GoodReads {goodreadsId:{param4}}) SET sb:Source:_SourceBook:SourceBook:GoodReads, sb.title = {param5}, sb.releaseDate = {param6}, sb.source = {param7} MERGE (sb)-[:SOURCE_FORMAT]->(sbf) WITH DISTINCT sbf, sa, sb, scs MATCH (sbf)-[rse:SOURCE_EDITION]->(sa) DELETE rse WITH DISTINCT sbf, sa, sb, scs OPTIONAL MATCH (sa)<-[rsc:SOURCE_CONTRIBUTED]-() DELETE rsc WITH DISTINCT sbf, sa, sb, scs REMOVE sbf:SourceBook:_SourceBook, sa:SourceEdition:_SourceEdition SET sbf:SourceBookFormat:_SourceBookFormat, sa:SourceAuthority:_SourceAuthority MERGE (sa)-[:SOURCE_FORMAT]->(sbf) WITH DISTINCT sbf, sb, scs UNWIND scs AS sc OPTIONAL MATCH (sc)-[rscf:SOURCE_CONTRIBUTED]->(sbf) WHERE NOT rscf IS NULL MERGE (sc)-[rscb:SOURCE_CONTRIBUTED]->(sb) SET rscb.role = rscf.role, rscb.roles = rscf.roles", map(
                        "param1", "",
                        "param2", "",
                        "param3", "",
                        "param4", "",
                        "param5", "",
                        "param6", "",
                        "param7", ""
                        ) );
            }
        };
    }
}

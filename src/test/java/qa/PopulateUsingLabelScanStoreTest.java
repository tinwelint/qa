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
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.schema.Schema.IndexState;

import static java.lang.Math.toIntExact;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.duration;

public class PopulateUsingLabelScanStoreTest
{
    private static final String PATH = "K:\\graph.db";
    private static enum Labels implements Label
    {
        One,
        Two,
        Three,
        Four,
        Five;
    }
    private static final Label[] LABELS = Labels.values();
    private static final String KEY = "key";

    @Test
    public void shouldAddLabelToAFewSelectNodes() throws Exception
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( PATH ) );
        for ( Label label : LABELS )
        {
            addLabelToRandomNodes( db, 1_000, label, random );
        }

        System.out.println( "Creating indexes" );
        try ( Transaction tx = db.beginTx() )
        {
            for ( Label label : Labels.values() )
            {
                db.schema().indexFor( label ).on( KEY ).create();
            }
            tx.success();
        }

        System.out.println( "Populating" );
        int totalAddedDuringPopulation = 0;
        long time = currentTimeMillis();
        while ( !allOnline( db ) )
        {
            int count = random.nextInt( 20 ) + 20;
            addLabelToRandomNodes( db, count, LABELS[random.nextInt( LABELS.length )], random );
            totalAddedDuringPopulation += count;
        }
        time = currentTimeMillis() - time;
        System.out.println( "Done " + totalAddedDuringPopulation + " added during population, took " + duration( time ) );

        db.shutdown();
    }

    @Test
    public void shouldBeTheSame() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( PATH ) );
        for ( Label label : LABELS )
        {
            compareLabels( db, label );
        }
        db.shutdown();
    }

    private void compareLabels( GraphDatabaseService db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<Node> lss = db.findNodes( label );

            long prev = -1;
            while ( lss.hasNext() )
            {
                long thisOne = lss.next().getId();
                if ( thisOne < prev )
                {
                    System.out.println( "unsorted prev:" + prev + ", this:" + thisOne );
                }
                prev = thisOne;
            }

//            Iterator<Node> ans = filter( node -> node.hasLabel( label ), db.getAllNodes().iterator() );

//            // Read whole set and compare
//            BitSet lssSet = toSet( lss );
//            BitSet ansSet = toSet( ans );
//            assertEquals( ansSet, lssSet );

            // Read iteratively (assuming sorted ids) and compare as we go
//            while ( lss.hasNext() || ans.hasNext() )
//            {
//                assertEquals( ans.next().getId(), lss.next().getId() );
//            }

            tx.success();
        }
    }

    private BitSet toSet( Iterator<Node> nodes )
    {
        BitSet set = new BitSet();
        while ( nodes.hasNext() )
        {
            set.set( toIntExact( nodes.next().getId() ) );
        }
        return set;
    }

    private void addLabelToRandomNodes( GraphDatabaseService db, int count, Label label, ThreadLocalRandom random )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < count; i++ )
            {
                Node node = db.getNodeById( random.nextLong( 500_000_000 ) );
                node.addLabel( label );
                node.setProperty( KEY, node.getId() );
            }
            tx.success();
        }
    }

    private boolean allOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                IndexState state = db.schema().getIndexState( index );
                if ( state == Schema.IndexState.POPULATING )
                {
                    return false;
                }
            }
            tx.success();
        }
        return true;
    }
}

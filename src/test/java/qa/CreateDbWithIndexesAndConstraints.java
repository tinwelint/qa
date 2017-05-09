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
import versiondiff.VersionDifferences;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.FileUtils;

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterators.first;

public class CreateDbWithIndexesAndConstraints
{
    @Test
    public void shouldCreateIt() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = VersionDifferences.newDb( cleared( "db-w-schema" ) );
        int numIndexes = 10;
        for ( int i = 0; i < numIndexes; i++ )
        {
            Label label1 = DynamicLabel.label( "Label" + i );
            Label label2 = DynamicLabel.label( "Label" + i + "_index" );
            String key = "key" + i;

            createUniquenessConstraint( db, label1, key );
            createIndex( db, label2, key );
        }
        awaitIndexesOnline( db );

        // WHEN
        for ( int r = 0, t = 0; r < 3; r++ )
        {
            System.out.println( r );
            for ( int i = 0; i < numIndexes; i++, t++ )
            {
                Label label1 = DynamicLabel.label( "Label" + i );
                Label label2 = DynamicLabel.label( "Label" + i + "_index" );
                String key = "key" + i;
                insertNodes( db, label1, key, 10_000, strings( t ) );
                insertNodes( db, label2, key, 10_000, strings( t ) );
                System.out.println( "  " + i );
            }
        }

        // THEN
        db.shutdown();
    }

    @Test
    public void shouldReadIt() throws Exception
    {
        GraphDatabaseService db = VersionDifferences.newDb( "db-w-schema" );

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                System.out.println( index );
                System.out.println( first( db.findNodes( index.getLabel(), single( index.getPropertyKeys() ),
                        strings( iOf( index.getLabel().name() ) ).apply( 0 ) ) ) );
            }
            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                System.out.println( constraint );
            }
            tx.success();
        }
        db.shutdown();
    }

    private int iOf( String key )
    {
        return Integer.parseInt( key.substring( 0, "key".length() ) );
    }

    private Function<Integer,Object> numbers()
    {
        return new Function<Integer,Object>()
        {
            @Override
            public Object apply( Integer from )
            {
                return from;
            }
        };
    }

    private Function<Integer,Object> floats()
    {
        return new Function<Integer,Object>()
        {
            @Override
            public Object apply( Integer from )
            {
                return from.floatValue();
            }
        };
    }

    private Function<Integer,Object> strings( final int base )
    {
        return new Function<Integer,Object>()
        {
            @Override
            public Object apply( Integer from )
            {
                return base + "_" + from.intValue();
            }
        };
    }

    private void insertNodes( GraphDatabaseService db, Label label, String key, int count,
            Function<Integer,Object> values )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < count; i++ )
            {
                Node node = db.createNode( label );
                Object value = values.apply( i );
                node.setProperty( key, value );
                System.out.println( label + ":" + key + "=" + value );
            }
            tx.success();
        }
    }

    private void awaitIndexesOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, MINUTES );
            tx.success();
        }
    }

    private void createIndex( GraphDatabaseService db, Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( key );
            tx.success();
        }
    }

    private void createUniquenessConstraint( GraphDatabaseService db, Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }
    }

    private String cleared( String string ) throws IOException
    {
        FileUtils.deleteRecursively( new File( string ) );
        return string;
    }
}

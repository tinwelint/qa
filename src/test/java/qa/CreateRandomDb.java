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

import versiondiff.VersionDifferences;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;

public class CreateRandomDb
{
    private static final Random random = new Random();

    public static void main( String[] args ) throws IOException
    {
        GraphDatabaseService db = VersionDifferences.newDbBuilder(
                cleared(
                        "target/randomdb"
                        )
                )
//                .setConfig( GraphDatabaseSettings.record_format, HighLimit.NAME )
                .newGraphDatabase();
        try
        {
            Node[] nodes = new Node[10_000];
            Relationship[] rels = new Relationship[nodes.length*10];
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes.length; i++ )
                {
                    nodes[i] = db.createNode( randomLabels() );
                    setProperties( nodes[i] );
                    db.createNode();
                }
                for ( int i = 0; i < rels.length; i++ )
                {
                    Relationship rel = nodes[random.nextInt( nodes.length )].createRelationshipTo(
                            nodes[random.nextInt( nodes.length )], randomType() );
                    setProperties( rel );
                    rels[i] = rel;
                }
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                int deleteCount = rels.length - rels.length/3;
                for ( int i = 0; i < deleteCount; i++ )
                {
                    int index = random.nextInt( rels.length );
                    Relationship rel = rels[index];
                    if ( rel != null )
                    {
                        rel.delete();
                        rels[index] = null;
                    }
                }
                tx.success();
            }
        }
        finally
        {
//            System.exit( 1 );
            db.shutdown();
        }
    }

    private static void setProperties( PropertyContainer entity )
    {
        int count = random.nextInt( 3 );
        for ( int i = 0; i < count; i++ )
        {
            entity.setProperty( randomKey(), randomValue() );
        }
    }

    private static Object randomValue()
    {
        return "value";
    }

    private static String randomKey()
    {
        return "key-" + random.nextInt( 10 );
    }

    private static RelationshipType randomType()
    {
        return DynamicRelationshipType.withName( "Type-" + random.nextInt( 5 ) );
    }

    private static Label[] randomLabels()
    {
        Label[] labels = new Label[random.nextInt( 3 )];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = DynamicLabel.label( "Label-" + random.nextInt( 10 ) );
        }
        return labels;
    }

    private static String cleared( String string ) throws IOException
    {
        FileUtils.deleteRecursively( new File( string ) );
        return string;
    }
}

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

import com.google.common.base.Optional;
import org.junit.Test;
import rx.Observable;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.neo_workbench.command_bench.model.Inventory;
import org.neo4j.neo_workbench.command_bench.model.numbers.AscendingExcludedNumbers;
import org.neo4j.neo_workbench.command_bench.model.numbers.Range;
import org.neo4j.neo_workbench.command_bench.model.results.ResultContext;
import org.neo4j.neo_workbench.command_bench.web.client.api.ClientError;
import org.neo4j.neo_workbench.command_bench.web.client.api.ClientResult;
import org.neo4j.neo_workbench.command_bench.web.client.api.ClientTransaction;
import org.neo4j.neo_workbench.command_bench.web.client.api.CypherClient;
import org.neo4j.neo_workbench.command_bench.web.client.api.CypherResponse;
import org.neo4j.neo_workbench.command_bench.web.client.api.OperationSafety;
import org.neo4j.neo_workbench.command_bench.web.client.api.Row;
import org.neo4j.neo_workbench.command_bench.web.client.api.RowNode;
import org.neo4j.neo_workbench.command_bench.web.client.api.RowValue;
import org.neo4j.quality_tasks.soaktesting.workloads.social.extended.commands.AddUser;

import static java.lang.Math.toIntExact;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.Format.duration;

public class SoakTestAddUserRegressionTest
{
    @Test
    public void shouldAddUser() throws Exception
    {
        GraphDatabaseService db = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase(
                new File( "K:\\extended\\3.0\\graph.db" ) );

        Inventory userIds = new Inventory( Range.minMax( 2_000_026_002, Integer.MAX_VALUE ) );
        AscendingExcludedNumbers deletedUsers = new AscendingExcludedNumbers( userIds.asRange() );
        CypherClient cypherClient = new EmbeddedCypherClient( db );
        AddUser command = new AddUser( userIds, deletedUsers, cypherClient,
                readMoveIds( db ), readReferencePlaces( db ) );

        try
        {
            long time = currentTimeMillis();
            int count = 1;
            ResultContext resultContext = new StupidResultContext();
            int success = 0;
            int failure = 0;
            for ( int i = 0; i < count; i++ )
            {
                System.out.println( "Executing query " + i );
                try
                {
                    command.execute( resultContext, i );
                    success++;
                }
                catch ( Exception e )
                {
                    failure++;
                    if ( failure == 1 )
                    {
                        e.printStackTrace();
                    }
                    System.out.println( e );
                }
            }
            time = currentTimeMillis() - time;
            System.out.println( duration( time ) + " for " + count + " queries, which means " +
                    (count/(time/1_000D)) + " q/s successes:" + success + " failures:" + failure );
        }
        finally
        {
            db.shutdown();
        }
    }

    private List<String> readMoveIds( GraphDatabaseService db )
    {
        List<String> movieIds = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            try ( ResourceIterator<Node> movies = db.findNodes( DynamicLabel.label( "Movie" ) ) )
            {
                while ( movies.hasNext() )
                {
                    movieIds.add( movies.next().getProperty( "imdbId" ).toString() );
                }
            }
            tx.success();
        }
        System.out.println( "read movies " + movieIds );
        return movieIds;
    }

    private List<Integer> readReferencePlaces( GraphDatabaseService db )
    {
        List<Integer> referencePlaces = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            try ( ResourceIterator<Node> places = db.findNodes( DynamicLabel.label( "ReferencePlace" ) ) )
            {
                while ( places.hasNext() )
                {
                    referencePlaces.add( toIntExact( places.next().getId() ) );
                }
            }
            tx.success();
        }
        System.out.println( "read places " + referencePlaces );
        return referencePlaces;
    }

    private static class EmbeddedCypherClient implements CypherClient
    {
        private final GraphDatabaseService db;

        public EmbeddedCypherClient( GraphDatabaseService db )
        {
            this.db = db;
        }

        @Override
        public void close() throws Exception
        {
        }

        @Override
        public Observable<CypherResponse> executeRead( String cypher )
        {
            return null;
        }

        @Override
        public Observable<CypherResponse> executeRead( String cypher, Map<String,Object> params )
        {
            return null;
        }

        @Override
        public Observable<CypherResponse> executeWrite( String cypher )
        {
            Result result = db.execute( cypher );
            return Observable.from( array( new ResultCypherResponse( result ) ) );
        }

        @Override
        public Observable<CypherResponse> executeWrite( String cypher, Map<String,Object> params )
        {
            Result result = db.execute( cypher, params );
            System.out.println( result.getExecutionPlanDescription() );
            return Observable.from( array( new ResultCypherResponse( result ) ) );
        }

        private String rulePlanner( String cypher )
        {
            return "CYPHER planner=rule " + cypher;
        }

        @Override
        public Observable<CypherResponse> executeSchemaChange( String cypher )
        {
            return null;
        }

        @Override
        public ClientTransaction createTransaction( OperationSafety context )
        {
            return null;
        }

        @Override
        public String description()
        {
            return null;
        }
    }

    private static class ResultCypherResponse implements CypherResponse
    {
        private final Result result;

        public ResultCypherResponse( Result result )
        {
            this.result = result;
        }

        @Override
        public boolean isSuccessful()
        {
            return true;
        }

        @Override
        public Collection<ClientResult> results()
        {
            return null;
        }

        @Override
        public ClientResult onlyResult()
        {
            return null;
        }

        @Override
        public Collection<Row> rows()
        {
            Collection<Row> rows = new ArrayList<>();
            while ( result.hasNext() )
            {
                rows.add( new ResultRow( result.next() ) );
            }
            return rows;
        }

        @Override
        public Row onlyRow()
        {
            if ( result.hasNext() )
            {
                return new ResultRow( result.next() );
            }
            return null;
        }

        @Override
        public Optional<Row> optionalRow()
        {
            return Optional.fromNullable( onlyRow() );
        }

        @Override
        public Collection<ClientError> errors()
        {
            return Collections.emptyList();
        }

        @Override
        public URI requestUri()
        {
            return null;
        }
    }

    private static class ResultRow implements Row
    {
        private final Map<String,Object> data;

        public ResultRow( Map<String,Object> data )
        {
            this.data = data;
        }

        @Override
        public String getString( String column )
        {
            return data.get( column ).toString();
        }

        @Override
        public int getInt( String column )
        {
            return (Integer) data.get( column );
        }

        @Override
        public long getLong( String column )
        {
            return (Long) data.get( column );
        }

        @Override
        public List<? extends RowValue> values()
        {
            List<RowValue> values = new ArrayList<>();
            for ( Map.Entry<String,Object> entry : data.entrySet() )
            {
                values.add( new ResultRowValue( entry.getValue() ) );
            }
            return values;
        }

        @Override
        public RowValue get( String column )
        {
            return null;
        }
    }

    private static class ResultRowValue implements RowValue
    {
        private final Object value;

        public ResultRowValue( Object value )
        {
            this.value = value;
        }

        @Override
        public String asString()
        {
            return value.toString();
        }

        @Override
        public int asInt()
        {
            return (Integer) value;
        }

        @Override
        public long asLong()
        {
            return (Long) value;
        }

        @Override
        public List<? extends RowValue> asCollection()
        {
            return null;
        }

        @Override
        public RowNode asNode()
        {
            return null;
        }

        @Override
        public boolean isNull()
        {
            return value == null;
        }
    }

    private static class StupidResultContext implements ResultContext
    {
        @Override
        public void setSuccess( String description )
        {
        }

        @Override
        public void setSuccess( Object description )
        {
        }

        @Override
        public void setFailed( String description )
        {
        }

        @Override
        public void setFailed( Throwable e )
        {
        }

        @Override
        public void setFailed( String description, Throwable e )
        {
        }

        @Override
        public void setFailed( Object description )
        {
        }

        @Override
        public void setIgnored( String description )
        {
        }

        @Override
        public void setIgnored( Object description )
        {
        }
    }
}

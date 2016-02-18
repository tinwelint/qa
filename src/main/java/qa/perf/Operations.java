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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.neo4j.graphdb.Transaction;

public class Operations
{
    private Operations()
    {
        throw new AssertionError();
    }

    public static <T extends Target> Supplier<Operation<T>> single( final Operation<T> operation )
    {
        return new Supplier<Operation<T>>()
        {
            @Override
            public Operation<T> get()
            {
                return operation;
            }
        };
    }

    public static <T extends Target> Operation<T> noop()
    {
        return new Operation<T>()
        {
            @Override
            public void perform( T on )
            {   // No-op
            }
        };
    }

    public static <T extends Target> Supplier<Operation<T>> multipleRandom(
            Object... alternatingOperationAndChance )
    {
        return multipleRandom( ThreadLocalRandom.current(), alternatingOperationAndChance );
    }

    public static <T extends Target> Supplier<Operation<T>> multipleRandom( final Random random,
            final Object... alternatingOperationAndChance )
    {
        final int totalChance = totalChance( alternatingOperationAndChance );
        return new Supplier<Operation<T>>()
        {
            @Override
            public Operation<T> get()
            {
                float selection = random.nextFloat();
                int collectiveChance = 0;
                for ( int i = 0; i < alternatingOperationAndChance.length; i++ )
                {
                    @SuppressWarnings( "unchecked" )
                    Operation<T> candidateOperation = (Operation<T>) alternatingOperationAndChance[i++];
                    collectiveChance += ((Number)alternatingOperationAndChance[i]).intValue();
                    float candidateChance = (float) collectiveChance / (float) totalChance;
                    if ( candidateChance >= selection )
                    {
                        return candidateOperation;
                    }
                }
                throw new IllegalStateException( "Should not happen" );
            }
        };
    }

    public static <T extends GraphDatabaseTarget> Supplier<Operation<T>> inTx( Supplier<Operation<T>> operations )
    {
        return () -> {
            return inTx( operations.get() );
        };
    }

    public static <T extends GraphDatabaseTarget> Operation<T> inTx( Operation<T> operation )
    {
        return (on) -> {
            try ( Transaction tx = on.db.beginTx() )
            {
                operation.perform( on );
                tx.success();
            }
        };
    }

    private static int totalChance( Object... alternatingOperationAndChance )
    {
        int totalChance = 0;
        for ( int i = 1; i < alternatingOperationAndChance.length; i += 2 )
        {
            totalChance += ((Number)alternatingOperationAndChance[i]).intValue();
        }
        return totalChance;
    }
}

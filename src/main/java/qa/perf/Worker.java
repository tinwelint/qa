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
package qa.perf;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class Worker<T extends Target> extends Thread
{
    private final T target;
    private final Supplier<Operation<T>> operation;
    private final AtomicBoolean end;
    private int completedCount;
    private final int batchSize;

    public Worker( String name, T target, Supplier<Operation<T>> operation, int batchSize, AtomicBoolean end )
    {
        super( name );
        this.target = target;
        this.operation = operation;
        this.batchSize = batchSize;
        this.end = end;
    }

    @Override
    public void run()
    {
        while ( !end.get() )
        {
            for ( int i = 0; i < batchSize; i++ )
            {
                operation.get().perform( target );
            }
            completedCount += batchSize;
        }
    }

    public int getCompletedCount()
    {
        return completedCount;
    }
}

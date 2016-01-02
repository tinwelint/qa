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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ComparatorTest
{
    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        List<Long> yo = new ArrayList<>();
        yo.add( 5L );
        yo.add( 2L );
        yo.add( 10L );

        Collections.sort( yo, new Comparator<Long>()
        {
            @Override
            public int compare( Long o1, Long o2 )
            {
                return -o1.compareTo( o2 );
            }
        } );

        for ( long l : yo )
        {
            System.out.println( l );
        }
    }
}

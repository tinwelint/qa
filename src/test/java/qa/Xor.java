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

public class Xor
{
    @Test
    public void shouldTest() throws Exception
    {
        long v1 = 7349534785L;
        long v2 = 84759879983L;
        long xored1 = v1 ^ v2;

        long extractedV1 = xored1 ^ v2;
        long extractedV2 = xored1 ^ v1;

        if ( v1 == extractedV1 )
        {
            System.out.println( "Extracted " + v1 + " " + extractedV1 );
        }
        if ( v2 == extractedV2 )
        {
            System.out.println( "Extracted " + v2 + " " + extractedV2 );
        }
    }
}

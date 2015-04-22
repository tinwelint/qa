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
package qa.temp;

import java.io.IOException;

/**
 * This class exists so that this qa project, which might have a different version than the one
 * currently checked out in git in the neo4j repo, and will just start the DumpLogicalLog class¨
 * for the neo4j version that this qa project specifies.
 */
public class DumpLogicalLog
{
    public static void main( String[] args ) throws IOException
    {
        org.neo4j.kernel.impl.util.DumpLogicalLog.main( args );
    }
}

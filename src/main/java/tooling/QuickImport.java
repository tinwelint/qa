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
package tooling;

import java.io.File;
import java.io.IOException;

import static org.neo4j.helpers.Args.parse;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

public class QuickImport
{
    public static void main( String[] args ) throws IOException
    {
        clean( args );
        org.neo4j.tooling.QuickImport.main( args );
    }

    private static void clean( String[] args ) throws IOException
    {
        deleteRecursively( new File( parse( args ).get( "into" ) ) );
    }
}

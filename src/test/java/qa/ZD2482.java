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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class ZD2482
{
    @Test
    public void shouldTest() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "zd2482/db" );

        // WHEN
        System.out.print( "Executing first" );
        db.execute( "USING PERIODIC COMMIT LOAD CSV FROM 'file:zd2482/data/8dd0c8b6-fd34-4df6-a3ab-8c9337248073.csv' AS line FIELDTERMINATOR '\t' CREATE (a:i635653886820412195 {name: line[0], cn: line[1], distinguishedname: line[2], objectGUID: line[3], objectSID: line[4], sAMAccountName: line[5], sAMAccountType: line[6], grouptype: line[7], useraccountcontrol: line[8], accountExpires: line[9], givenName: line[10], lastLogon: line[11], lastLogonTimeStamp: line[12], primaryGroupID: line[13], sn: line[14], type: line[15], adspath: line[16], userprincipalname: line[17], description: line[18], displayname: line[19], whencreated: line[20], whenchanged: line[21], sIDHistory1: line[22], sIDHistory2: line[23], sIDHistory3: line[24], sIDHistory4: line[25], sIDHistory5: line[26], domain: line[27], isactive: line[28], ad: line[29], tokenfactor: line[30], commonsid: line[31]}) return count(a)" );
        System.out.println( " DONE" );

        System.out.print( "Executing second" );
        db.execute( "USING PERIODIC COMMIT LOAD CSV FROM 'file:zd2482/data/e96ad6c1-7351-49d7-b711-ac2fdadded50.csv' AS line FIELDTERMINATOR '\t' match (a1:i635653886820412195 { objectSID: line[0]}), (a2:i635653886820412195 { objectSID: line[1]}) create (a1)<-[r1:rel_member]-(a2) return count(r1)" );
        System.out.println( " DONE" );

        // THEN
        db.shutdown();
    }
}

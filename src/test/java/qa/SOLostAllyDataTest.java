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

import org.apache.commons.io.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class SOLostAllyDataTest
{
    public static void main( String[] args )
    {
        String theString = IOUtils.toString( request.getInputStream(), "UTF-8" );
        System.out.println( "Salesforce data/n " + theString );
        //gets request input stream
        Transaction tx = null;
        try
        {
            System.out.println( " " + request.getContentType() );
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource is = new InputSource();
            Map<String,String> update = new HashMap<>();
            is.setCharacterStream( new StringReader( theString ) );
            System.out.println( "After parsing XML" );
            Document doc = db.parse( is );
            doc.getDocumentElement().normalize();
            Element ele = doc.getDocumentElement();
            Node node = doc.getDocumentElement();
            System.out.println( "Root element " + doc.getDocumentElement() );
            //get fields from xml objects
            NodeList sObject = doc.getElementsByTagName( "sObject" );
            GraphDatabaseService ndb = null;
            tx = ndb.beginTx();
            for ( int j = 0; j < sObject.getLength(); j++ )
            {
                Node rowNode = sObject.item( j );
                String salesforceObj = rowNode.getAttributes().getNamedItem( "xsi:type" ).getNodeValue();
                System.out.println( salesforceObj );
                if ( salesforceObj.equalsIgnoreCase( "sf:Farmer_Biodata__c" ) )
                {
                    farmerID = getXmlNodeValue( "sf:Id", ele );
                    agentId = getXmlNodeValue( "sf:CreatedById", ele );
                    bb = biodataModel.getBiodata( "Id", farmerID );
                    if ( null != bb )
                    {
                        System.out.println( "Farmer Already Exist Id " + farmerID );
                        out.println( sendAck() );
                    }
                    else
                    {
                        org.neo4j.graphdb.Node biodataNode = ndb.createNode();
                        biodataNode.addLabel( Labels.FARMER );
                        for ( int k = 0; k < rowNode.getChildNodes().getLength(); k++ )
                        {
                            if ( rowNode.getChildNodes().item( k ).getNodeName().equals( "sf:Id" )
                                    || rowNode.getChildNodes().item( k ).getNodeName().equals( "sf:CreatedById" ) )
                            {
                                System.out.println(
                                        "id : " + getObjectFieldId( rowNode.getChildNodes().item( k ).getNodeName() ) );
                                biodataNode.setProperty(
                                        getObjectFieldId( rowNode.getChildNodes().item( k ).getNodeName() ),
                                        rowNode.getChildNodes().item( k ).getTextContent() );
                            }
                            if ( !rowNode.getChildNodes().item( k ).getNodeName().equals( "sf:Id" )
                                    && !rowNode.getChildNodes().item( k ).getNodeName().equals( "#text" )
                                    && !rowNode.getChildNodes().item( k ).getNodeName().equals( "sf:CreatedById" ) )
                            {
                                System.out.println(
                                        getObjectFieldName( rowNode.getChildNodes().item( k ).getNodeName() ) );
                                biodataNode.setProperty(
                                        getObjectFieldName( rowNode.getChildNodes().item( k ).getNodeName() ),
                                        rowNode.getChildNodes().item( k ).getTextContent() );
                            }
                        }
                        biodataNode.setProperty( Biodata.LAST_MODIFIED, new Date().getTime() );
                        System.out.printf( "new node created {0}", biodataNode.getId() );
                        out.println( sendAck() );
                    }
                    tx.success();
                }
            }
        }
        catch ( Exception ex )
        {
            ex.printStackTrace();
        }
        finally
        {
            tx.close();
        }
    }
}

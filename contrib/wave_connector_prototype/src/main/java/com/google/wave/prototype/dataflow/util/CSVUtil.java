package com.google.wave.prototype.dataflow.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.ws.bind.TypeMapper;
import com.sforce.ws.parser.XmlOutputStream;

/**
 * Utility to convert Salesforce SObject into CSV
 * It requires SOQL to get the field queried from Salesforce
 */
public class CSVUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CSVUtil.class);
    // "SELECT ".length()
    private static final int SELECT_PREFIX_LENGTH = 7;

    /** Fields queried from Salesforce */
    private String[] queryFields;

    /**
     * @param soqlQuery - SOQL query used to fetch Salesforce Reference data
     */
    public CSVUtil(String soqlQuery) {
        // Parsing the SOQL Query to get the fields queried from Salesforce
        // Removing select clause
        // Example query for better understanding in each steps
        // soqlQuery = "SELECT oppor.AccountId, oppor.Id FROM Opportunity as oppor"
        soqlQuery = soqlQuery.substring(SELECT_PREFIX_LENGTH);
        // Getting the fields present between select and from
        int fromIndex = StringUtils.indexOfIgnoreCase(soqlQuery, " from ");
        // soqlQuery = "oppor.AccountId, oppor.Id "
        soqlQuery = soqlQuery.substring(0, fromIndex);
        // Fields are separated by comma
        // Splitting will give the queried fields
        // fields = {"oppor.AccountId", " oppor.Id"}
        // space is taken care by stripAlias()
        queryFields = soqlQuery.split(",");
        LOG.debug("Fields from SOQL Query " + queryFields);
    }

    /**
     * @param sObject One of the result on executing SOQL Query
     * @return Converted CSV data from SObject
     * @throws Exception
     */
    public String getAsCSV(SObject sObject) throws Exception {
        StringBuilder csv = new StringBuilder();

        // Reading the SObject as XML Document
        Document doc = readDocument(sObject);
        // Reading the fields present in XML document
        Map<String, String> fieldMap = readFields(doc);
        for (int i = 0; i < queryFields.length; i++) {
            if (i != 0) {
                csv.append(',');
            }

            // Getting the corresponding value from the fieldMap using fields constructed from SOQL query
            String fieldValue = fieldMap.get(stripAlias(queryFields[i]));
            if (fieldValue != null) {
                csv.append(fieldValue);
            }
        }

        // Completing a row
        csv.append('\n');

        LOG.debug("Returning CSV " + csv);
        return csv.toString();
    }

    private Map<String, String> readFields(Document doc) {
        // XML will be like
        // <sObject>
        //   <Opportunity>
        //     <AccountId>1233</AcccountId>
        //	   <OpportunityId>1234</OpportunityId>
        //	   <ProposalId>101</ProposalId>
        //	 </Opportunity>
        // <sObject>
        // Here doc is <sObject>
        Node parentElement = doc.getChildNodes().item(0);
        // Here parentElement is <Opportunity>
        NodeList childNodes = parentElement.getChildNodes();
        // Child Nodes are <AccountId>, <OpportunityId> and <ProposalId>
        Map<String, String> fieldValueMap = new HashMap<String, String>();
        if (childNodes != null && childNodes.getLength() > 0) {
            for (int i = 0, size = childNodes.getLength(); i < size; i++) {
                Node item = childNodes.item(i);
                // Removing prefix as the column name present in SOQL will not have it
                // This nodename will be compared with fields queried in SOQL
                fieldValueMap.put(stripPrefix(item.getNodeName()), item.getTextContent());
            }
        }

        return fieldValueMap;
    }

    private String stripPrefix(String nodeName) {
        return strip(nodeName, ':');
    }

    private String stripAlias(String field) {
        return strip(field, '.').trim();
    }

    private String strip(String str, char separator) {
        int aliasIndex = str.indexOf(separator);
        if (aliasIndex != -1) {
            return str.substring(aliasIndex + 1);
        }

        return str;
    }

    private Document readDocument(SObject sObject) throws Exception  {
        ByteArrayInputStream bis = null;
        XmlOutputStream xmlOutputStream = null;

        try {
            // Getting the doc as <sObject/>
            // As Salesforce SOAP API is used converting to XML is the only option
            QName element = new QName("urn:sobject", "result");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            xmlOutputStream = new XmlOutputStream(bos, false);
            xmlOutputStream.startDocument();
            // Writes all the fields to outputStream
            sObject.write(element, xmlOutputStream, new TypeMapper());
            xmlOutputStream.endDocument();

            bis = new ByteArrayInputStream(bos.toByteArray());
            // Converting it as DOM object
            DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = builderFactory.newDocumentBuilder();
            return docBuilder.parse(bis);
        } catch (ParserConfigurationException | SAXException e) {
            throw new Exception(e);
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException ioe) {
                    LOG.warn("Error while closing Stream", ioe);
                }

                if (xmlOutputStream != null) {
                    // This will make sure the ByteArrayOutputStream provided is also closed
                    try {
                        xmlOutputStream.close();
                    } catch (IOException ioe) {
                        LOG.warn("Error while closing Stream", ioe);
                    }
                }
            }
        }
    }

}

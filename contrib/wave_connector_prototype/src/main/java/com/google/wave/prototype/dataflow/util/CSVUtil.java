package com.google.wave.prototype.dataflow.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

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

    /** Columns queried from Salesforce */
    private List<String> columnNames = new ArrayList<String>();

    /**
     * @param soqlQuery - SOQL query used to fetch Salesforce Reference data
     * @throws Exception
     */
    public CSVUtil(String soqlQuery) throws Exception {
        // Parsing the SOQL Query to get the columns queried from Salesforce
        Select stmt = (Select) CCJSqlParserUtil.parse(soqlQuery);
        PlainSelect plainSelect = (PlainSelect) stmt.getSelectBody();
        // SelectedItems contains the column to be selected
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            // We will get only columns as expressions are not supported
            Column column = (Column) ((SelectExpressionItem) selectItem).getExpression();
            columnNames.add(column.getColumnName());
        }

        LOG.debug("Columns from SOQL Query " + columnNames);
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
        for (int i = 0, size = columnNames.size(); i < size; i++) {
            if (i != 0) {
                csv.append(',');
            }

            // Getting the corresponding value from the fieldMap using columns constructed from SOQL query
            String fieldValue = fieldMap.get(columnNames.get(i));
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

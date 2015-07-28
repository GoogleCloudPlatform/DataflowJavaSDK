package com.google.wave.prototype.dataflow.sf;

import static com.google.wave.prototype.dataflow.util.SFConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.wave.prototype.dataflow.BaseTest;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.util.SFConstants;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * Unit test for {@link SFWaveDatasetWriter}
 */
public class SFWaveDatasetWriterTest extends BaseTest {
    private static final String DUMMY_METADATA_CONTENT = "dummy_metadata_content";
    private static final String DUMMY_DATASET_CONTENT = "dummy_dataset_content";
    private static final String DUMMY_SOBJECT_ID = "dummy_sobject_id";
    private static final String DUMMY_DATASET_NAME = "dummy_dataset_name";

    private static int CREATE_CALL_COUNT = 0;
    private static int UPDATE_CALL_COUNT = 0;

    private SFConfig sfConfig;

    @Before
    public void setup() throws Exception {
        StringBuilder metadataFileLocationSB = new StringBuilder();
        metadataFileLocationSB.append(SFConstants.LOCAL_FILE_PREFIX);
        metadataFileLocationSB.append(System.getProperty("user.dir"));
        metadataFileLocationSB.append("/test_metadata.json");

        sfConfig = mock(SFConfig.class);

        when(sfConfig.createPartnerConnection()).thenReturn(PartnerConnectionExt.getInstance());

        CREATE_CALL_COUNT = 0;
        UPDATE_CALL_COUNT = 0;
    }

    @Test
    public void testWrite() throws Exception {
        SFWaveDatasetWriter writer = new SFWaveDatasetWriter(sfConfig, DUMMY_DATASET_NAME);
        String sfObjId = writer.write(DUMMY_METADATA_CONTENT.getBytes(), DUMMY_DATASET_CONTENT.getBytes());

        assertEquals(DUMMY_SOBJECT_ID, sfObjId);
        // Verify that PartnerConnection.create() has been called twice
        // metadata publish and datapart publish
        assertEquals(2, CREATE_CALL_COUNT);

        // Verify that PartnerConnection.update() has been called only once
        // finalize publish
        assertEquals(1, UPDATE_CALL_COUNT);
    }

    public static class PartnerConnectionExt extends PartnerConnection {

        public static PartnerConnectionExt getInstance() throws ConnectionException {
            ConnectorConfig config = new ConnectorConfig();
            config.setUsername("dummy_sf_user");
            config.setPassword("dummy_sf_password");
            config.setManualLogin(true);
            // Salesforce SOAP API checks for /services/Soap/c/
            config.setServiceEndpoint("http://dummysgendpoint/services/Soap/u/");
            return new PartnerConnectionExt(config);
        }

        public PartnerConnectionExt(ConnectorConfig config)
                throws ConnectionException {
            super(config);
        }

        @Override
        public SaveResult[] update(SObject[] sObjects)
                throws ConnectionException {
            int expectedSObjectCount = 1;
            assertEquals(expectedSObjectCount, sObjects.length);

            String type = sObjects[0].getType();
            assertEquals(STR_INSIGHTS_EXTERNAL_DATA, type);

            // verify action
            String actualAction = (String) sObjects[0].getField(STR_ACTION);
            assertEquals(STR_ACTION_PROCESS, actualAction);

            // verify Sobject Id
            assertEquals(DUMMY_SOBJECT_ID, sObjects[0].getId());

            UPDATE_CALL_COUNT++;
            return constructSaveResultArray();
        }

        @Override
        public SaveResult[] create(SObject[] sObjects)
                throws ConnectionException {
            int expectedSObjectCount = 1;
            assertEquals(expectedSObjectCount, sObjects.length);

            String type = sObjects[0].getType();
            assertNotNull(type);
            // It is metadata publish
            if (STR_INSIGHTS_EXTERNAL_DATA.equals(type)) {
                // verify dataset name
                String actualDatasetName = (String) sObjects[0].getField(STR_EDGEMART_ALIAS);
                assertEquals(DUMMY_DATASET_NAME, actualDatasetName);

                // verify metadata content
                byte[] actualMetadataContent = (byte[]) sObjects[0].getField(STR_METADATA_JSON);
                assertEquals(DUMMY_METADATA_CONTENT, new String(actualMetadataContent));
            } else if (STR_INSIGHTS_EXTERNAL_DATA_PART.equals(type)) {
                // verify dataset content
                byte[] actualDatasetContent = (byte[]) sObjects[0].getField(STR_DATAFILE);
                assertEquals(DUMMY_DATASET_CONTENT, new String(actualDatasetContent));

                // verify sobject id
                String actualSObjectId = (String) sObjects[0].getField(STR_INSIGHTS_EXTERNAL_DATA_ID);
                assertEquals(DUMMY_SOBJECT_ID, actualSObjectId);
            } else {
                fail("PartnerConnection.create() called with invalid type " + type);
            }

            CREATE_CALL_COUNT++;
            return constructSaveResultArray();
        }

        @Override
        public void logout() throws ConnectionException {
            // no op
        }

        private SaveResult[] constructSaveResultArray() {
            SaveResult saveResult = new SaveResult();
            saveResult.setId(DUMMY_SOBJECT_ID);
            saveResult.setSuccess(true);

            return new SaveResult[] {saveResult};
        }
    }
}

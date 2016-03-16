package utils;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Lists;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;
import java.util.List;

/**
 * Created by ppastuszka on 14.12.15.
 */
public class BQ {
    private final Bigquery bigquery;

    BQ() {
        try {
            JacksonFactory jaksonFactory = JacksonFactory.getDefaultInstance();
            NetHttpTransport httpTransport = newTrustedTransport();
            GoogleCredential credential = GoogleCredential.getApplicationDefault
                    (httpTransport, jaksonFactory).createScoped(BigqueryScopes.all());
            this.bigquery = new Bigquery.Builder(httpTransport, jaksonFactory, credential).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static BQ get() {
        return Holder.INSTANCE;
    }

    public Table createTable(TableReference reference, TableSchema tableSchema) throws IOException {
        return bigquery.tables().insert(
                reference.getProjectId(),
                reference.getDatasetId(),
                new Table().setTableReference(reference).setSchema(tableSchema)
        ).execute();
    }

    public void deleteTableIfExists(TableReference reference) throws IOException {
        try {
            bigquery.tables().delete(
                    reference.getProjectId(),
                    reference.getDatasetId(),
                    reference.getTableId()).execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getDetails().getCode() != 404) {
                throw e;
            }
        }
    }

    public List<String> readAllFrom(TableReference reference) throws IOException {
        List<TableRow> rows = Lists.newArrayList();
        String pageToken = null;
        TableDataList callResult;
        do {
            callResult = bigquery.tabledata().list
                    (reference.getProjectId(),
                            reference.getDatasetId(),
                            reference.getTableId()).setPageToken(pageToken).execute();
            pageToken = callResult.getPageToken();

            if (callResult.getRows() != null) {
                rows.addAll(callResult.getRows());
            }
        } while (pageToken != null);

        List<String> columnValues = Lists.newArrayList();
        for (TableRow row : rows) {
            columnValues.add((String) row.getF().get(0).getV());
        }
        return columnValues;
    }

    private static class Holder {
        private static final BQ INSTANCE = new BQ();
    }
}

package utils;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Lists;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeScopes;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Zone;
import com.google.api.services.compute.model.ZoneList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by ppastuszka on 14.12.15.
 */
public class GCE {
    private static final Logger LOG = LoggerFactory.getLogger(GCE.class);
    private final Compute compute;

    GCE() {
        try {
            JacksonFactory jaksonFactory = JacksonFactory.getDefaultInstance();
            NetHttpTransport httpTransport = newTrustedTransport();
            GoogleCredential credential = GoogleCredential.getApplicationDefault
                    (httpTransport, jaksonFactory).createScoped(ComputeScopes.all());
            this.compute = new Compute.Builder(httpTransport, jaksonFactory, credential).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static GCE get() {
        return Holder.INSTANCE;
    }

    public List<Zone> listZones(String project) throws IOException {
        List<Zone> zones = Lists.newArrayList();
        String pageToken = null;

        do {
            ZoneList result = compute.zones().list(project).setPageToken(pageToken).execute();
            zones.addAll(result.getItems());
            pageToken = result.getNextPageToken();
        } while (pageToken != null);
        return zones;
    }

    public void stopInstance(Instance instance) throws IOException, InterruptedException {
        String[] zoneNameParts = instance.getZone().split("/");
        String zone = zoneNameParts[zoneNameParts.length - 1];
        String project = zoneNameParts[zoneNameParts.length - 3];
        Operation response = compute.instances().stop(project, zone, instance.getName()).execute();

        waitUntilDone(project, zone, response);
    }

    public void startInstance(Instance instance) throws IOException, InterruptedException {
        String[] zoneNameParts = instance.getZone().split("/");
        String zone = zoneNameParts[zoneNameParts.length - 1];
        String project = zoneNameParts[zoneNameParts.length - 3];
        Operation response = compute.instances().start
                (project, zone, instance.getName()).execute();

        waitUntilDone(project, zone, response);
    }

    private void waitUntilDone(String project, String zone, Operation operation) throws IOException,
            InterruptedException {
        handleOperationErrors(operation);
        while (!operation.getStatus().equals("DONE")) {
            LOG.info("Waiting for operation to finish. Current state is {}", operation.getStatus());
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            operation = compute.zoneOperations().get(project, zone, operation.getName()).execute();
            handleOperationErrors(operation);
        }
    }

    private void handleOperationErrors(Operation response) {
        if (response.getHttpErrorStatusCode() != null) {
            throw new RuntimeException(response.getHttpErrorStatusCode() + ": " + response
                    .getHttpErrorMessage());
        }
        if (response.getError() != null) {
            Operation.Error.Errors error = response.getError().getErrors().get(0);
            throw new RuntimeException(error.getMessage());
        }
        if (response.getWarnings() != null) {
            for (Operation.Warnings x : response.getWarnings()) {
                LOG.warn(x.getMessage());
            }
        }
    }

    public List<Instance> listInstances(String project) throws IOException {
        List<Instance> instances = Lists.newArrayList();
        for (Zone zone : listZones(project)) {
            String pageToken = null;
            do {
                InstanceList result = compute.instances().list(project, zone.getName())
                        .setPageToken(pageToken).execute();
                pageToken = result.getNextPageToken();
                if (result.getItems() != null) {
                    instances.addAll(result.getItems());
                }
            } while (pageToken != null);
        }
        return instances;
    }

    private static class Holder {
        private static final GCE INSTANCE = new GCE();
    }
}

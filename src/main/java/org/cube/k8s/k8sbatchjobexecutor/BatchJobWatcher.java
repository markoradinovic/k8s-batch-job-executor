package org.cube.k8s.k8sbatchjobexecutor;

import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.cube.k8s.k8sbatchjobexecutor.Utils.nullSafeInt;

@SuppressWarnings("WeakerAccess")
public class BatchJobWatcher implements Watcher<Job> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchJobWatcher.class);

    private final BatchJobWatcherCallback callback;

    public BatchJobWatcher(BatchJobWatcherCallback callback) {
        this.callback = callback;
    }

    @Override
    public void eventReceived(Action action, Job job) {
        LOG.info("Batch Job: " + action.name());

        if (job != null && job.getStatus() != null) {
            if (nullSafeInt(job.getStatus().getSucceeded()) > 0) {
                LOG.info("Batch Job [" + job.getMetadata().getName() + "] Succeeded.");
                LOG.info(job.getStatus().toString());
                callback.onJobCompleted(0, job);

            } else if (nullSafeInt(job.getStatus().getFailed()) > 0) {
                LOG.error("Batch Job [" + job.getMetadata().getName() + "] Failed.");
                LOG.info(job.getStatus().toString());
                callback.onJobCompleted(1, job);
            } else {
                LOG.info("Batch Job Active: [" + nullSafeInt(job.getStatus().getActive()) + "]");
            }
        }

    }

    @Override
    public void onClose(KubernetesClientException cause) {
        LOG.error(cause.getMessage(), cause);
    }
}

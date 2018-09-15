package org.cube.k8s.k8sbatchjobexecutor;

import io.fabric8.kubernetes.api.model.batch.Job;

public interface BatchJobWatcherCallback {

    void onJobCompleted(int statusCode, Job job);
}

package org.cube.k8s.k8sbatchjobexecutor;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class BatchJobExecutorService implements ApplicationRunner, BatchJobWatcherCallback {

    private static final Logger LOG = LoggerFactory.getLogger(BatchJobExecutorService.class);

    private KubernetesClient client;

    private Watch watcher;
    private Path logPath = Paths.get("");

    @Override
    public void run(ApplicationArguments args) throws Exception {
        validateArguments(args);
        configureLogPath(args);
        createKubernetesClient();
        runAndWatchBatchJob(args.getNonOptionArgs().get(0));
    }

    @Override
    public void onJobCompleted(int statusCode, Job job) {
        saveJobLog(job);
        LOG.info("Deleting Batch Job [" + job.getMetadata().getName() + "].");
        client.resource(job).delete();
        LOG.info("Batch Job [" + job.getMetadata().getName() + "] Deleted.");
        LOG.info("Closing Watcher");
        watcher.close();
        LOG.info("Watcher Closed");
        closeKubernetesClient();
        System.exit(statusCode);
    }

    private void configureLogPath(ApplicationArguments args) {
        if (args.containsOption("logPath")) {
            String logPathLocation = args.getOptionValues("logPath").get(0);
            LOG.info("Batch Job Log Path: [" + logPathLocation + "]");
            logPath = Paths.get(logPathLocation);
            logPath.toFile().mkdirs();
        }
    }

    private void validateArguments(ApplicationArguments args) throws IOException {
        if (args.getNonOptionArgs().size() != 1) {
            LOG.error("Invalid BatchJob config file. Unknown file.");
            System.exit(1);
        }
        if (Files.notExists(Paths.get(args.getNonOptionArgs().get(0)))) {
            LOG.error("Invalid BatchJob config file. File not found. " + args.getNonOptionArgs().get(0));
            System.exit(1);
        }
        LOG.info("Using Batch Job config file: " + args.getNonOptionArgs().get(0));

        if (LOG.isDebugEnabled()) {
            System.out.println(new String(Files.readAllBytes(Paths.get(args.getNonOptionArgs().get(0)))));
        }
    }

    private void createKubernetesClient() {
        client = new DefaultKubernetesClient();
        LOG.info("Connected to k8s Cluster using API " + client.getApiVersion() + " to master: " + client.getConfiguration().getMasterUrl());
        if (StringUtils.isEmpty(client.getNamespace())) {
            client.getConfiguration().setNamespace("default");
            LOG.warn("Namespace not provided. Using 'default' namespace.");
        } else {
            LOG.info("Using namespace: " + client.getNamespace());
        }
    }

    private void closeKubernetesClient() {
        LOG.info("Disconnecting from k8s cluster.");
        if (client != null) {
            client.close();
        }
        LOG.info("Connection closed.");
    }

    private void runAndWatchBatchJob(String batchSpecYaml) {
        Job job = client.batch().jobs().load(batchSpecYaml).create();
        watcher = client.resource(job).watch(new BatchJobWatcher(this));
    }


    private void saveJobLog(Job job) {
        PodList podList = client.pods().withLabel("job-name", job.getMetadata().getName()).list();
        if (podList != null && !podList.getItems().isEmpty()) {
            podList.getItems().forEach(this::savePodLog);
        }
    }

    private void savePodLog(Pod pod) {
        try {
            File logFile = new File(logPath.toFile(), pod.getMetadata().getName() + ".log");
            LOG.info("Saving Batch Job log [" + logFile.getAbsolutePath() + "].");
            FileOutputStream logFileOutputStream = new FileOutputStream(logFile);
            Reader podLogReader = client.pods()
                    .inNamespace(pod.getMetadata().getNamespace())
                    .withName(pod.getMetadata().getName())
                    .getLogReader();
            IOUtils.copy(podLogReader, logFileOutputStream, Charset.forName("UTF8"));
            logFileOutputStream.flush();
            logFileOutputStream.close();
        } catch (IOException e) {
            LOG.error("Unable to write Batch Job Pod Log file.", e);
        }
    }

}

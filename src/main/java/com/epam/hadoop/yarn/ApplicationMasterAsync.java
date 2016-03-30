package com.epam.hadoop.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {

    private final static Log LOG = LogFactory.getLog(ApplicationMasterAsync.class);

    private Configuration configuration;
    private String inputFilePath;
    private String outputFilePath;
    private String jarPath;
    private NMClient nmClient;
    private AtomicInteger containersNumToWait;
    private AtomicInteger launchedContainers;

    public ApplicationMasterAsync(String inputFile, String outputFile, int containersNum, String jarFile) {
        inputFilePath = inputFile;
        outputFilePath = outputFile;
        jarPath = jarFile;
        containersNumToWait = new AtomicInteger(containersNum);
        launchedContainers = new AtomicInteger(0);
        configuration = new YarnConfiguration();
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus containerStatus: statuses) {
            LOG.info("[AM] Completed container " + containerStatus.getContainerId());
            containersNumToWait.decrementAndGet();
        }
    }

    public void onContainersAllocated(List<Container> containers) {
        LOG.info("[AM] Containers allocated");
        for (Container container: containers) {
            try {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                //set commands
                ctx.setCommands(Collections.singletonList("$JAVA_HOME/bin/java"
                                + " -Xmx128M"
                                + " com.epam.hadoop.yarn.Crawler"
                                + " " + getInputFilePath()
                                + " " + getOutputFilePath()
                                + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
                addJarAsLocalResource(ctx, new Path(getJarPath()));

                //create env
                Map<String, String> env = new HashMap();
                for (String classpathElem: getConfiguration().getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                    Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), classpathElem.trim(), File.pathSeparator);
                }
                Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), ApplicationConstants.Environment.PWD.$() + File.separator + "*", File.pathSeparator);
                ctx.setEnvironment(env);

                LOG.info("[AM] Launching container " + container.getId());

                nmClient.startContainer(container, ctx);
            } catch (Exception e) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + e);
            }
        }
    }

    public void addJarAsLocalResource(ContainerLaunchContext context, Path jar) throws IOException {
        LocalResource containerJar = Records.newRecord(LocalResource.class);
        FileStatus jarFileStatus = FileSystem.get(getConfiguration()).getFileStatus(jar);
        containerJar.setResource(ConverterUtils.getYarnUrlFromPath(jar));
        containerJar.setType(LocalResourceType.FILE);
        containerJar.setTimestamp(jarFileStatus.getModificationTime());
        containerJar.setSize(jarFileStatus.getLen());
        containerJar.setVisibility(LocalResourceVisibility.PUBLIC);
        context.setLocalResources(Collections.singletonMap("simple2.jar", containerJar));
    }

    public void onShutdownRequest() {

    }

    public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    public float getProgress() {
        return 0;
    }

    public void onError(Throwable e) {

    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getOutputFilePath() {
        return outputFilePath;
    }

    public String getJarPath() {
        return jarPath;
    }

    public AtomicInteger getContainersAmount() {
        return containersNumToWait;
    }

    public AtomicInteger getLaunchedContainers() {
        return launchedContainers;
    }

    public boolean doneWithContainers() {
        return containersNumToWait.get() == 0;
    }

    public void runMainLoop() throws Exception {
        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        //register application master with resource manager
        LOG.info("[AM] register application master 0");
        rmClient.registerApplicationMaster("", 0, "");
        LOG.info("[AM] register application master 1");

        //set priorities for worker containers
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        FileSystem fs = FileSystem.get(configuration);
        Path inputPath = new Path(getInputFilePath());
        if (fs.exists(inputPath)) {
            FileStatus fileStatus = fs.getFileStatus(inputPath);

            //try to run container on node with file blocks location
            BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            BlockLocation firstBlockLocation = blockLocations[0];
            LOG.debug("First block location info: " + firstBlockLocation);
            String[] hosts = firstBlockLocation.getHosts();

            //make container requests to resource manager
            for (int i = 0; i < containersNumToWait.get(); i++) {
                ContainerRequest containerRequest = new ContainerRequest(capability, hosts, null, priority);
                LOG.info("[AM] Making resource request " + i);
                rmClient.addContainerRequest(containerRequest);
            }

            while (!doneWithContainers()) {
                Thread.sleep(1000);
            }

            LOG.info("[AM] unregister application master 0");
            //unregister application master with resource manager
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            LOG.info("[AM] unregister application master 1");
        } else {
            LOG.error("Input file: " + getInputFilePath() + " is not exist. No need to run containers");
        }



    }

    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        int containersNum = 1;
        String jarFile = args[2];

        ApplicationMasterAsync appMaster = new ApplicationMasterAsync(inputFile, outputFile, containersNum, jarFile);
        appMaster.runMainLoop();
    }
}

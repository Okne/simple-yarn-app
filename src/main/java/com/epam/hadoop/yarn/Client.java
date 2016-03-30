package com.epam.hadoop.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Client {

    private final static Log LOG = LogFactory.getLog(Client.class);

    public void run(String[] args) throws Exception {
        //retrieve passed params
        String inputFile = args[0];
        String outputFile = args[1];
        String jarFile = args[2];
        Path jarPath = new Path(jarFile);

        //create client
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        //create client application
        YarnClientApplication yarnClientApp = yarnClient.createApplication();
        yarnClientApp.getClass().getResourceAsStream("hello.txt");

        //create application master launch context
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
                Collections.singletonList("$JAVA_HOME/bin/java"
                        + " -Xmx128M"
                        + " com.epam.hadoop.yarn.ApplicationMasterAsync"
                        + " " + inputFile
                        + " " + outputFile
                        //+ " " + numContainers
                        + " " + jarFile
                        + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                        + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
        );

        //setup jar for application master
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        FileStatus jarFileStatus = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarFileStatus.getLen());
        jarFileStatus.getLen();
        appMasterJar.setTimestamp(jarFileStatus.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
        amContainer.setLocalResources(Collections.singletonMap("simpleapp.jar", appMasterJar));

        //setup CLASSPATH for application master
        Map<String, String> appMasterEnv = new HashMap();
        //add yarn CLASSPATH elements
        for (String classpathElem: conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv,
                    Environment.CLASSPATH.name(),
                    classpathElem.trim(),
                    File.pathSeparator);
        }
        //add pwd dir to CLASSPATH
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*", File.pathSeparator);
        amContainer.setEnvironment(appMasterEnv);

        //set up resources requirement for application master
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        //set up application submission context
        ApplicationSubmissionContext appContext = yarnClientApp.getApplicationSubmissionContext();
        appContext.setApplicationName("simple-yarn-app");
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");

        //log application id
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application: " + appId);

        //submit application
        yarnClient.submitApplication(appContext);

        //reporting
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED
                || appState != YarnApplicationState.KILLED
                || appState != YarnApplicationState.FAILED) {
            Thread.sleep(1000);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        LOG.info("Application " + appId + " finished with state "
                + appState + " at " + appReport.getFinishTime());

    }

    public static void main(String[] args) {
        try {
            Client client = new Client();
            client.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
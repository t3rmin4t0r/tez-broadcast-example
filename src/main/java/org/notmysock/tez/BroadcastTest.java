package org.notmysock.tez;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import java.net.URL;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

public class BroadcastTest extends Configured implements Tool {
  
  public static class InputProcessor extends SimpleProcessor {
    Text word = new Text();

    public InputProcessor(ProcessorContext context) {
      super(context);
      ByteBuffer userPayload = getContext().getUserPayload().getPayload();
      // TODO: do something with this 
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getOutputs().size() == 1);
      for(LogicalOutput out: getOutputs().values()) {
        if(out instanceof UnorderedKVOutput) {
          UnorderedKVOutput output = (UnorderedKVOutput) out;
          KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
          kvWriter.write(word, new IntWritable(getContext().getTaskIndex()));
        }
      }      
    }   
  }

  public static class MapOneProcessor extends SimpleProcessor {
    Text word = new Text();

    public MapOneProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(inputs.size() == 1);
      KeyValueReader broadcastKvReader = (KeyValueReader) getInputs().get("Broadcast").getReader();
      int sum = 0;
      while (broadcastKvReader.next()) {
        sum += ((IntWritable) broadcastKvReader.getCurrentValue()).get();
      }
    }
  }

  private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
      Path stagingDir, Map<String, LocalResource> localFiles)
      throws IOException, YarnException {

    int numBroadcastTasks = 1;

    byte[] procByte = { 0 };
    UserPayload procPayload = UserPayload.create(ByteBuffer.wrap(procByte));

    Vertex broadcastVertex = Vertex.create("Broadcast", ProcessorDescriptor.create(
        InputProcessor.class.getName()), numBroadcastTasks);
    broadcastVertex.setTaskLocalFiles(localFiles);

    Vertex mapVertex = Vertex.create("Map 1",
        ProcessorDescriptor.create(
            MapOneProcessor.class.getName()).setUserPayload(procPayload), 20);
    mapVertex.setTaskLocalFiles(localFiles);

    UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName()).build();

    DAG dag = new DAG("BroadcastExample");
    
    dag.addVertex(broadcastVertex)
        .addVertex(mapVertex)
        .addEdge(Edge.create(broadcastVertex, mapVertex, edgeConf.createDefaultBroadcastEdgeProperty()));
    return dag;
  }

  public static Path getCurrentJarURL() throws URISyntaxException {
    return new Path(BroadcastTest.class.getProtectionDomain().getCodeSource()
        .getLocation().toURI());
  }
  
  private LocalResource createLocalResource(FileSystem fs, Path file) throws IOException {

    final LocalResourceType type = LocalResourceType.FILE;
    final LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;

    FileStatus fstat = fs.getFileStatus(file);

    org.apache.hadoop.yarn.api.records.URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    LocalResource lr = Records.newRecord(LocalResource.class);
    lr.setResource(resourceURL);
    lr.setType(type);
    lr.setSize(resourceSize);
    lr.setVisibility(visibility);
    lr.setTimestamp(resourceModificationTime);

    return lr;
  }
  
  public boolean run(Configuration conf, boolean doLocalityCheck) throws Exception {
    System.out.println("Running BroadcastTest");
    // conf and UGI
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    UserGroupInformation.setConfiguration(tezConf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR
        + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
        + Path.SEPARATOR + Long.toString(System.currentTimeMillis());    
    Path stagingDir = new Path(stagingDirStr);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = fs.makeQualified(stagingDir);
    
    Path jobJar = new Path(stagingDir, "job.jar");
    fs.copyFromLocalFile(getCurrentJarURL(), jobJar);
    
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put("job.jar", createLocalResource(fs, jobJar));

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    TezClient tezSession = null;
    // needs session or else TaskScheduler does not hold onto containers
    tezSession = TezClient.create("BroadcastTest", tezConf);
    tezSession.addAppMasterLocalResources(localResources);
    tezSession.start();

    DAGClient dagClient = null;

    try {
        DAG dag = createDAG(fs, tezConf, stagingDir, localResources);

        tezSession.waitTillReady();
        dagClient = tezSession.submitDAG(dag);

        // monitoring
        DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
          return false;
        }
        return true;
    } finally {
      fs.delete(stagingDir, true);
      tezSession.stop();
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    boolean doLocalityCheck = true;
    if (args.length == 1) {
      if (args[0].equals(skipLocalityCheck)) {
        doLocalityCheck = false;
      } else {
        printUsage();
        throw new TezException("Invalid command line");
      }
    } else if (args.length > 1) {
      printUsage();
      throw new TezException("Invalid command line");
    }
    boolean status = run(getConf(), doLocalityCheck);
    return status ? 0 : 1;
  }
  
  private static void printUsage() {
    System.err.println("broadcastAndOneToOneExample " + skipLocalityCheck);
    ToolRunner.printGenericCommandUsage(System.err);
  }
  
  static String skipLocalityCheck = "-skipLocalityCheck";

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    BroadcastTest job = new BroadcastTest();
    int status = ToolRunner.run(conf, job, args);
    System.exit(status);
  }
}
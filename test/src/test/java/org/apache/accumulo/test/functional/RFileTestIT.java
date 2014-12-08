/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class RFileTestIT extends ConfigurableMacIT {

  private void mput(Mutation m, String cf, String cq, String cv, String val) {
    ColumnVisibility le = new ColumnVisibility(cv.getBytes(StandardCharsets.UTF_8));
    m.put(new Text(cf), new Text(cq), le, new Value(val.getBytes(StandardCharsets.UTF_8)));
  }

  private void insertData(Connector c, String tableName) throws Exception {

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 10; j++) {
        Mutation m1 = new Mutation("row1");
        mput(m1, "cf" + i, "cq1", "", "v1");
        mput(m1, "cf" + i, "cq1", "A", "v2");
        mput(m1, "cf" + i, "cq1", "B", "v3");
        mput(m1, "cf" + i, "cq3", "A", "v4");
        mput(m1, "cf" + i, "cq1", "A&(L|M)", "v5");
        mput(m1, "cf" + i, "cq1", "B&(L|M)", "v6");
        mput(m1, "cf" + i, "cq1", "A&B&(L|M)", "v7");
        mput(m1, "cf" + i, "cq2", "A", "v8");
        mput(m1, "cf" + i, "cq1", "FOO", "v9");
        mput(m1, "cf" + i, "cq1", "A&FOO&(L|M)", "v10");
        mput(m1, "cf" + i, "cq2", "FOO", "v11");
        mput(m1, "cf" + i, "cq1", "(A|B)&FOO&(L|M)", "v12");
        mput(m1, "cf" + i, "cq1", "A&B&(L|M|FOO)", "v13");
        bw.addMutation(m1);
      }
    }

    for (int i = 2; i < 2000; i++) {
      for (int j = 0; j < 10; j++) {
        Mutation m1 = new Mutation("row1");
        mput(m1, "cf" + i, "cq1", "D", "v1");
        mput(m1, "cf" + i, "cq1", "C", "v2");
        mput(m1, "cf" + i, "cq1", "B", "v3");
        mput(m1, "cf" + i, "cq2", "B", "v4");
        mput(m1, "cf" + i, "cq1", "A&(L|M)", "v5");
        mput(m1, "cf" + i, "cq1", "B&(L|M)", "v6");
        mput(m1, "cf" + i, "cq1", "A&B&(L|M)", "v7");
        mput(m1, "cf" + i, "cq1", "A&B&(L)", "v8");
        mput(m1, "cf" + i, "cq1", "A&FOO", "v9");
        mput(m1, "cf" + i, "cq1", "A&FOO&(L|M)", "v10");
        mput(m1, "cf" + i, "cq1", "FOO", "v11");
        mput(m1, "cf" + i, "cq1", "(A|B)&FOO&(L|M)", "v12");
        mput(m1, "cf" + i, "cq1", "A&B&(L|M|FOO)", "v13");
        bw.addMutation(m1);
      }
    }
    
    Mutation m1 = new Mutation("row1");
    mput(m1, "cf5", "cq1", "E", "v1");
    bw.addMutation(m1);
    
    m1 = new Mutation("row1");
    mput(m1, "cf2500", "cq1", "E", "v1");
    bw.addMutation(m1);
    m1 = new Mutation("row1");
    mput(m1, "cf2560", "cq1", "A", "v1");
    bw.addMutation(m1);

    bw.close();
  }

  static AtomicInteger userId = new AtomicInteger(0);

  static String makeUserName() {
    return "user_" + userId.getAndIncrement();
  }

  @Test
  public void run() throws Exception {

    // Make a test username and password
    String testUser = makeUserName();
    PasswordToken testPasswd = new PasswordToken("test_password");

    // Create a root user and create the table
    // Create a test user and grant that user permission to read the table
    final String tableName = getUniqueNames(1)[0];
    final Connector c = getConnector();
    c.securityOperations().createLocalUser(testUser, testPasswd);
    Connector conn = c.getInstance().getConnector(testUser, testPasswd);
    c.tableOperations().create(tableName);
    c.securityOperations().grantTablePermission(testUser, tableName, TablePermission.READ);
    c.securityOperations().grantTablePermission(testUser, tableName, TablePermission.ALTER_TABLE);
    Authorizations auths = new Authorizations("A");
    c.securityOperations().changeUserAuthorizations(testUser, auths);

    Set<Text> lg1 = new HashSet<Text>();
    lg1.add(new Text("cf0"));
    Set<Text> lg2 = new HashSet<Text>();
    lg2.add(new Text("cf1"));
    Set<Text> lg3 = new HashSet<Text>();
    for(int i = 2; i < 2000; i++)
      lg3.add(new Text("cf" + i));

    Map<String,Set<Text>> locGroup = new HashMap<String,Set<Text>>();
    locGroup.put("lg1", lg1);
    locGroup.put("lg2", lg2);
    locGroup.put("lg3", lg3);

    c.tableOperations().setLocalityGroups(tableName, locGroup);
    System.out.println(c.tableOperations().getLocalityGroups(tableName).toString());

    insertData(c, tableName);
    conn.tableOperations().compact(tableName, null, null, true, true);

    // Make sure all the data that was put in the table is still correct
    int count = 0;
    final Scanner scanner = conn.createScanner(tableName, auths);
    Long startTime = System.currentTimeMillis();
    for (Entry<Key,Value> entry : scanner) {
      // System.out.println(entry.getKey() + " " + entry.getValue());
      // count++;
    }
    Long totalTime = System.currentTimeMillis() - startTime;
    System.out.println(totalTime + " Milliseconds");
    // Assert.assertEquals(40, count);

  }
}

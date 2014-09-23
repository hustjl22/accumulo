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
package org.apache.accumulo.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fs.PreferredVolumeChooser;
import org.apache.accumulo.test.functional.SimpleMacIT;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class CreateTableWithNewTableConfigIT extends SimpleMacIT {

  protected int defaultTimeoutSeconds() {
    return 30;
  };
  
  public int numProperties(Connector connector, String tableName) throws AccumuloException, TableNotFoundException
  {
    int countNew = 0;
    for (Entry<String,String> entry : connector.tableOperations().getProperties(tableName)) {
      countNew++;
    }
    
    return countNew;
  }
  
  public int compareProperties(Connector connector, String tableNameOrig, String tableName, String changedProp) throws AccumuloException, TableNotFoundException
  {
    boolean inNew = false;
    int countOrig = 0;
    for (Entry<String,String> orig : connector.tableOperations().getProperties(tableNameOrig)) {
      countOrig++;
      for (Entry<String,String> entry : connector.tableOperations().getProperties(tableName)) {
        if (entry.equals(orig)) {
          inNew = true;
          break;
        } else if (entry.getKey().equals(orig.getKey()) && !entry.getKey().equals(changedProp))
          Assert.fail("Property " + orig.getKey() + " has different value than deprecated method");
      }
      if (!inNew)
        Assert.fail("Original property missing after using the new create method");
    }
    return countOrig;
  }
  

  @Test
  public void tableNameOnly() throws Exception {
    log.info("Starting tableNameOnly");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, new NewTableConfiguration());

    String tableNameOrig = "original";
    connector.tableOperations().create(tableNameOrig);
    
    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);
    
    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
  }

  @Test
  public void tableNameAndLimitVersion() throws Exception {
    log.info("Starting tableNameAndLimitVersion");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    boolean limitVersion = false;
    connector.tableOperations().create(tableName, new NewTableConfiguration().setLimitVersion(limitVersion));

    String tableNameOrig = "originalWithLimitVersion";
    connector.tableOperations().create(tableNameOrig, limitVersion);
    
    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);
    
    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
  }

  @Test
  public void tableNameLimitVersionAndTimeType() throws Exception {
    log.info("Starting tableNameLimitVersionAndTimeType");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    boolean limitVersion = false;
    TimeType tt = TimeType.LOGICAL;
    connector.tableOperations().create(tableName, new NewTableConfiguration().setLimitVersion(limitVersion).setTimeType(tt));

    String tableNameOrig = "originalWithLimitVersionAndTimeType";
    connector.tableOperations().create(tableNameOrig, limitVersion, tt);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);
    
    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
  }

  @Test
  public void addCustomPropAndChangeExisting() throws Exception {
    log.info("Starting addCustomPropAndChangeExisting");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<String,String>();
    String propertyName = Property.TABLE_VOLUME_CHOOSER.getKey();
    String volume = PreferredVolumeChooser.class.getName();
    properties.put(propertyName, volume);

    String propertyName2 = "table.custom.testProp";
    String volume2 = "Test property";
    properties.put(propertyName2, volume2);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, new NewTableConfiguration().setProperties(properties));

    String tableNameOrig = "originalWithTableName";
    connector.tableOperations().create(tableNameOrig);
    
    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, propertyName);

    Assert.assertEquals("Extra properties using the new create method", countOrig + 1, countNew);
  }

}

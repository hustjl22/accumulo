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
package org.apache.accumulo.core.client.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.admin.TimeType;

/**
 * 
 */
public class NewTableConfiguration {

  private static final TimeType DEFAULT_TIME_TYPE = TimeType.MILLIS;
  private TimeType timeType = null;

  private boolean limitVersion = true;

  private static final Map<String,String> DEFAULT_PROPERTIES = new HashMap<String,String>();
  private Map<String,String> properties = null;

  public NewTableConfiguration setTimeType(TimeType tt) {
    this.timeType = tt;
    return this;
  }

  public TimeType getTimeType() {
    return timeType != null ? timeType : DEFAULT_TIME_TYPE;
  }

  public NewTableConfiguration setLimitVersion(boolean lv) {
    this.limitVersion = lv;
    return this;
  }

  public boolean getLimitVersion() {
    return limitVersion;
  }

  public NewTableConfiguration setProperties(Map<String,String> prop) {
    this.properties = prop;
    return this;
  }

  public Map<String,String> getProperties() {
    return properties != null ? properties : DEFAULT_PROPERTIES;
  }
}

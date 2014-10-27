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
package org.apache.accumulo.server.fs;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.AccumuloConfiguration.AllFilter;
import org.apache.accumulo.core.conf.AccumuloConfiguration.PropertyFilter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

public class PerTableVolumeChooser extends RandomVolumeChooser implements VolumeChooser {
  private static final Logger log = Logger.getLogger(PerTableVolumeChooser.class);

  public PerTableVolumeChooser() {}

  @Override
  public String choose(Optional<VolumeChooserEnvironment> env, String[] options) {
    if (env.isPresent()) {
      String clazzName = new String();

      // Get the current table's properties, and find the chooser property
      PropertyFilter filter = new AllFilter();
      Map<String,String> props = new HashMap<String,String>();
      env.get().getProperties(props, filter);

      clazzName = props.get(Property.TABLE_VOLUME_CHOOSER.getKey());

      try {
        // Load the correct chooser and create an instance of it
        Class<? extends VolumeChooser> clazz = AccumuloVFSClassLoader.loadClass(clazzName, VolumeChooser.class);
        VolumeChooser instance = clazz.newInstance();
        // Choose the volume based using the created chooser
        return instance.choose(env, options);
      } catch (Exception e) {
        // If an exception occurs, first write a warning and then use a default RandomVolumeChooser to choose from the given options
        log.warn("PerTableVolumeChooser failed because class " + clazzName + " did not exist. Defaulting back to the RandomVolumeChooser", e);
        return super.choose(Optional.<VolumeChooserEnvironment> absent(), options);
      }
    } else {
      return super.choose(Optional.<VolumeChooserEnvironment> absent(), options);
    }
  }
}

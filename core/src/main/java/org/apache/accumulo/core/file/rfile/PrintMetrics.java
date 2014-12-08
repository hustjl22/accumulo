/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 *
 */
public class PrintMetrics {
  private static final Logger log = Logger.getLogger(PrintMetrics.class);
  
  static class Opts extends Help {
	    @Parameter(names = {"--hash"}, description = "list visibility values as hashes")
	    boolean hash = false;
	    @Parameter(description = " <file> { <file> ... }")
	    List<String> files = new ArrayList<String>();
	  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    AccumuloConfiguration aconf = SiteConfiguration.getInstance(DefaultConfiguration.getInstance());
    FileSystem hadoopFs = VolumeConfiguration.getDefaultVolume(conf, aconf).getFileSystem();
    FileSystem localFs = FileSystem.getLocal(conf);
    Opts opts = new Opts();
    opts.parseArgs(PrintInfo.class.getName(), args);
    if (opts.files.isEmpty()) {
      System.err.println("No files were given");
      System.exit(-1);
    }

    for (String arg : opts.files) {
      Path path = new Path(arg);
      FileSystem fs;
      if (arg.contains(":"))
        fs = path.getFileSystem(conf);
      else {
        log.warn("Attempting to find file across filesystems. Consider providing URI instead of path");
        fs = hadoopFs.exists(path) ? hadoopFs : localFs; // fall back to local
      }

      System.out.println(arg);
      System.out.println();
      CachableBlockFile.Reader _rdr = new CachableBlockFile.Reader(fs, path, conf, null, null, aconf);
      Reader iter = new RFile.Reader(_rdr);

      ArrayList<ArrayList<ByteSequence>> localityGroupCF = iter.getLocalityGroupCF();

      int localityGroup = 0;
      for (ArrayList<ByteSequence> cf : localityGroupCF) {

        AtomicLongMap<String> visibilities = AtomicLongMap.create(new HashMap<String, Long>());
        long numKeys = 0;
        iter.seek(new Range((Key) null, (Key) null), cf, true);
        while (iter.hasTop()) {
          Key key = iter.getTopKey();
          Text vis = key.getColumnVisibility();
          if (visibilities.containsKey(vis.toString())) {
            visibilities.getAndIncrement(vis.toString());
          } else
            visibilities.put(vis.toString(), 1);

          iter.next();
          numKeys++;
        }
        
        
        Map<String, Long> blockData = iter.getVisibilities(localityGroup);
        int numBlocks = iter.getNumBlocks(localityGroup);
        System.out.println("Locality Group with column families: " + cf);
        System.out.println("Visibility " + "\t" + "Number of keys" + "\t   " + "Percent of keys" + "\t" + "Number of blocks" + "\t" + "Percent of blocks");
        for (Entry<String,Long> entry : visibilities.asMap().entrySet()) {          
          HashFunction hf = Hashing.md5();
          HashCode hc = hf.newHasher()
                 .putString(entry.getKey(), Charsets.UTF_8)
                 .hash();
          if(opts.hash)
        	  System.out.print(hc.toString().substring(0, 8) + "\t\t"  + entry.getValue() + "\t\t");
          else
        	  System.out.print(entry.getKey() + "\t\t"  + entry.getValue() + "\t\t");
          System.out.printf("%.2f", ((double) entry.getValue() / numKeys) * 100);
          System.out.print("%\t\t\t");

          long blocksIn = blockData.get(entry.getKey());

          System.out.print(blocksIn + "\t\t   ");
          System.out.printf("%.2f", ((double) blocksIn / numBlocks) * 100);
          System.out.print("%");

          System.out.println("");
        }
        System.out.println("Number of keys: " + numKeys);
        System.out.println();

        localityGroup++;
      }
    }

  }

}

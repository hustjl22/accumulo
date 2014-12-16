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
package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 * 
 */
public class VisMetricsGatherer {

  private ArrayList<AtomicLongMap<String>> visibilities;
  private ArrayList<AtomicLongMap<String>> blocks;
  private ArrayList<Long> numEntries;
  private ArrayList<Integer> numBlocks;
  private ArrayList<String> inBlock;
  private ArrayList<ArrayList<ByteSequence>> localityGroupCF;
  private RFile.Reader iter;
  private int numLG;

  public VisMetricsGatherer(RFile.Reader reader) {
    iter = reader;
    visibilities = new ArrayList<>();
    blocks = new ArrayList<>();
    localityGroupCF = iter.getLocalityGroupCF();
    numEntries = new ArrayList<>();
    numBlocks = new ArrayList<>();
    inBlock = new ArrayList<>();
    numLG = 0;
  }

  public void newLocalityGroup() {
    visibilities.add(AtomicLongMap.create(new HashMap<String,Long>()));
    blocks.add(AtomicLongMap.create(new HashMap<String,Long>()));
    numLG++;
    numEntries.add((long) 0);
    numBlocks.add(0);
  }

  public void addVisibility(String vis) {
    if (visibilities.get(numLG - 1).containsKey(vis.toString())) {
      visibilities.get(numLG - 1).getAndIncrement(vis.toString());
    } else
      visibilities.get(numLG - 1).put(vis.toString(), 1);

    numEntries.set(numLG - 1, numEntries.get(numLG - 1) + 1);

    if (!inBlock.contains(vis.toString()) && blocks.get(numLG - 1).containsKey(vis.toString())) {
      blocks.get(numLG - 1).incrementAndGet(vis.toString());
      inBlock.add(vis.toString());
    } else if (!inBlock.contains(vis.toString()) && !blocks.get(numLG - 1).containsKey(vis.toString())) {
      blocks.get(numLG - 1).put(vis.toString(), 1);
      inBlock.add(vis.toString());
    }

  }

  public void newBlock() {
    inBlock.clear();
    numBlocks.set(numLG - 1, numBlocks.get(numLG - 1) + 1);
  }

  public void printMetrics(boolean hash) {
    for (int i = 0; i < numLG; i++) {
      System.out.println("Locality Group with column families: " + localityGroupCF.get(i));
      System.out.printf("%-27s", "Visibility");
      System.out.println("Number of keys" + "\t   " + "Percent of keys" + "\t" + "Number of blocks" + "\t" + "Percent of blocks");
      for (Entry<String,Long> entry : visibilities.get(i).asMap().entrySet()) {
        HashFunction hf = Hashing.md5();
        HashCode hc = hf.newHasher().putString(entry.getKey(), Charsets.UTF_8).hash();
        if (hash)
          System.out.printf("%-20s", hc.toString().substring(0, 8));
        else
          System.out.printf("%-20s", entry.getKey());
        System.out.print("\t\t" + entry.getValue() + "\t\t\t");
        System.out.printf("%.2f", ((double) entry.getValue() / numEntries.get(i)) * 100);
        System.out.print("%\t\t\t");

        long blocksIn = blocks.get(i).get(entry.getKey());

        System.out.print(blocksIn + "\t\t   ");
        System.out.printf("%.2f", ((double) blocksIn / numBlocks.get(i)) * 100);
        System.out.print("%");

        System.out.println("");
      }
      System.out.println("Number of keys: " + numEntries.get(i));
      System.out.println();
    }
  }

}

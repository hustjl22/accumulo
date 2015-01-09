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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NewRFileTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  static {
    Logger.getLogger(org.apache.hadoop.io.compress.CodecPool.class).setLevel(Level.OFF);
    Logger.getLogger(org.apache.hadoop.util.NativeCodeLoader.class).setLevel(Level.OFF);
  }

  static class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {

    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (mark != 0)
        throw new IllegalStateException();

      reset();
      long skipped = skip(pos);

      if (skipped != pos)
        throw new IOException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {

      if (position >= buf.length)
        throw new IllegalArgumentException();
      if (position + length > buf.length)
        throw new IllegalArgumentException();
      if (length > buffer.length)
        throw new IllegalArgumentException();

      System.arraycopy(buf, (int) position, buffer, offset, length);
      return length;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      read(position, buffer, 0, buffer.length);

    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
    }

  }

  private static void checkIndex(Reader reader) throws IOException {
    FileSKVIterator indexIter = reader.getIndex();

    if (indexIter.hasTop()) {
      Key lastKey = new Key(indexIter.getTopKey());

      if (reader.getFirstKey().compareTo(lastKey) > 0)
        throw new RuntimeException("First key out of order " + reader.getFirstKey() + " " + lastKey);

      indexIter.next();

      while (indexIter.hasTop()) {
        if (lastKey.compareTo(indexIter.getTopKey()) > 0)
          throw new RuntimeException("Indext out of order " + lastKey + " " + indexIter.getTopKey());

        lastKey = new Key(indexIter.getTopKey());
        indexIter.next();

      }

      if (!reader.getLastKey().equals(lastKey)) {
        throw new RuntimeException("Last key out of order " + reader.getLastKey() + " " + lastKey);
      }
    }
  }

  public static class TestRFile {

    private Configuration conf = CachedConfiguration.getInstance();
    public RFile.Writer writer;
    private ByteArrayOutputStream baos;
    private FSDataOutputStream dos;
    private SeekableByteArrayInputStream bais;
    private FSDataInputStream in;
    private AccumuloConfiguration accumuloConfiguration;
    public Reader reader;
    public SortedKeyValueIterator<Key,Value> iter;

    public TestRFile(AccumuloConfiguration accumuloConfiguration) {
      this.accumuloConfiguration = accumuloConfiguration;
      if (this.accumuloConfiguration == null)
        this.accumuloConfiguration = AccumuloConfiguration.getDefaultConfiguration();
    }

    public void openWriter(boolean startDLG) throws IOException {

      baos = new ByteArrayOutputStream();
      dos = new FSDataOutputStream(baos, new FileSystem.Statistics("a"));
      CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(dos, "gz", conf, accumuloConfiguration);
      writer = new RFile.Writer(_cbw, 1000, 1000);

      if (startDLG)
        writer.startDefaultLocalityGroup();
    }

    public void openWriter() throws IOException {
      openWriter(true);
    }

    public void closeWriter() throws IOException {
      dos.flush();
      writer.close();
      dos.close();
      if (baos != null) {
        baos.close();
      }
    }

    public void openReader() throws IOException {

      int fileLength = 0;
      byte[] data = null;
      data = baos.toByteArray();

      bais = new SeekableByteArrayInputStream(data);
      in = new FSDataInputStream(bais);
      fileLength = data.length;

      LruBlockCache indexCache = new LruBlockCache(100000000, 100000);
      LruBlockCache dataCache = new LruBlockCache(100000000, 100000);

      CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(in, fileLength, conf, dataCache, indexCache, AccumuloConfiguration.getDefaultConfiguration());
      reader = new RFile.Reader(_cbr);
      iter = new ColumnFamilySkippingIterator(reader);

      checkIndex(reader);
    }

    public void closeReader() throws IOException {
      reader.close();
      in.close();
    }

    public void seek(Key nk) throws IOException {
      iter.seek(new Range(nk, null), EMPTY_COL_FAMS, false);
    }
  }

  static Key nk(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
  }

  static Value nv(String val) {
    return new Value(val.getBytes());
  }

  static String nf(String prefix, int i) {
    return String.format(prefix + "%06d", i);
  }

  public AccumuloConfiguration conf = null;

  public static Set<ByteSequence> ncfs(String... colFams) {
    HashSet<ByteSequence> cfs = new HashSet<ByteSequence>();

    for (String cf : colFams) {
      cfs.add(new ArrayByteSequence(cf));
    }

    return cfs;
  }

  @Test
  public void newTest() throws IOException {
    TestRFile trf = new TestRFile(conf);
    trf.openWriter(false);

    // For best case
    for (int i = 0; i < 26; i++) {
      trf.writer.startNewLocalityGroup("lg" + i, ncfs("cf" + i));
      for (int j = 0; j < 1000000; j++)
        trf.writer.append(nk("0000", "cf" + i, "cq", "" + ((char) ('A' + (i % 26))), 4), nv("v" + j));
    }

    // For worst case
/*  for (int i = 0; i < 26; i++) {
      trf.writer.startNewLocalityGroup("lg" + i, ncfs("cf" + i));
      for (int k = 0; k < 10; k++) {
        for (int j = 0; j < 10; j++)
          trf.writer.append(nk("0000", "cf" + i, "cq", ("" + k) + j, 4), nv("v" + j + i));
      }
      for (int j = 0; j < 1000000 - 100; j++)
        trf.writer.append(nk("0000", "cf" + i, "cq", "99", 4), nv("v" + j));
    }
*/
    trf.writer.close();
    trf.openReader();
    // trf.reader.printInfo();

    HashSet<ByteSequence> cfs = new HashSet<ByteSequence>();
    for (int i = 0; i < 26; i++) {
      cfs.add(new ArrayByteSequence("cf" + i));
    }

    int count = 0;
    Long startTime = System.currentTimeMillis();
    trf.iter.seek(new Range(), cfs, true);

    while (trf.iter.hasTop()) {
      trf.iter.next();
    }
    Long totalTime = System.currentTimeMillis() - startTime;
    System.out.println(totalTime + " Milliseconds");
    System.out.println(count);
    trf.closeReader();

  }

}

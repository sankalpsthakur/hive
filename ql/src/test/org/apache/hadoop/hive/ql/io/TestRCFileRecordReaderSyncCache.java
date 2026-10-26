/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HIVE-30192: RCFileRecordReader's JVM-wide sync cache must not reuse an offset
 * from a previous generation of the same path after that file is overwritten.
 */
public class TestRCFileRecordReaderSyncCache {

  private Configuration conf;
  private FileSystem fs;
  private Path dir;
  private Path file;

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    ColumnProjectionUtils.setReadAllColumns(conf);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_USE_RCFILE_SYNC_CACHE, true);
    RCFileOutputFormat.setColumnNumber(conf, 1);
    conf.setInt(HiveConf.ConfVars.HIVE_RCFILE_RECORD_INTERVAL.varname, 4);
    fs = FileSystem.getLocal(conf);
    dir = new Path(System.getProperty("test.tmp.dir", ".") + "/rcfile-sync-cache-overwrite");
    file = new Path(dir, "test_rcfile");
    cleanup();
    fs.mkdirs(dir);
  }

  @After
  public void teardown() throws Exception {
    cleanup();
  }

  private void cleanup() throws IOException {
    if (fs != null && dir != null) {
      fs.delete(dir, true);
    }
  }

  @Test
  public void testSameFileMultiSplitReadsAllRows() throws IOException {
    final int rows = 80;
    writeRows(file, rows, "same-", 256);
    long fileLen = fs.getFileStatus(file).getLen();
    long splitAt = fileLen / 2;
    assertTrue("file must be large enough to split", splitAt > 0 && splitAt < fileLen);

    int fromWholeFile = countRows(new FileSplit(file, 0, fileLen, new String[0]));
    int fromSplits = countRows(new FileSplit(file, 0, splitAt, new String[0]))
        + countRows(new FileSplit(file, splitAt, fileLen - splitAt, new String[0]));
    assertEquals(rows, fromWholeFile);
    assertEquals("sync cache must not drop rows of an unchanged file", rows, fromSplits);
  }

  @Test
  public void testOverwriteDoesNotSilentlyTruncateRows() throws IOException {
    final int firstRows = 80;
    final int secondRows = 130;

    writeRows(file, firstRows, "first-", 512);
    long firstLen = fs.getFileStatus(file).getLen();
    long splitAt = firstLen / 2;
    assertTrue("first file must be large enough to split", splitAt > 0 && splitAt < firstLen);

    // Populate the JVM-wide sync cache for path + splitAt from the first generation.
    int firstFromSplits = countRows(new FileSplit(file, 0, splitAt, new String[0]))
        + countRows(new FileSplit(file, splitAt, firstLen - splitAt, new String[0]));
    assertEquals(firstRows, firstFromSplits);

    // Overwrite the same path with a different size and different row-group layout.
    writeRows(file, secondRows, "second-", 192);
    long secondLen = fs.getFileStatus(file).getLen();
    assertTrue("overwritten file must still span the previous split boundary", secondLen > splitAt);
    assertTrue("overwritten file must change length", secondLen != firstLen);

    int fromWholeFile = countRows(new FileSplit(file, 0, secondLen, new String[0]));
    assertEquals(secondRows, fromWholeFile);

    // Read the non-leading split first so a stale cache hit is used before this
    // generation can replace the path+splitAt entry.
    int fromSecondSplit = countRows(
        new FileSplit(file, splitAt, secondLen - splitAt, new String[0]));
    int fromFirstSplit = countRows(new FileSplit(file, 0, splitAt, new String[0]));
    assertEquals("overwrite must not silently drop rows via a stale sync-cache offset",
        secondRows, fromFirstSplit + fromSecondSplit);
  }

  @Test
  public void testOverwriteWithSyncCacheDisabledReadsAllRows() throws IOException {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_USE_RCFILE_SYNC_CACHE, false);

    writeRows(file, 60, "nocache-a-", 256);
    long firstLen = fs.getFileStatus(file).getLen();
    long splitAt = firstLen / 2;
    countRows(new FileSplit(file, 0, splitAt, new String[0]));

    final int secondRows = 95;
    writeRows(file, secondRows, "nocache-b-", 128);
    long secondLen = fs.getFileStatus(file).getLen();
    assertTrue(secondLen > splitAt);

    int total = countRows(new FileSplit(file, splitAt, secondLen - splitAt, new String[0]))
        + countRows(new FileSplit(file, 0, splitAt, new String[0]));
    assertEquals(secondRows, total);
  }

  private void writeRows(Path path, int count, String prefix, int padBytes) throws IOException {
    fs.delete(path, false);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, path, null, null);
    BytesRefArrayWritable row = new BytesRefArrayWritable(1);
    byte[] pad = new byte[padBytes];
    Arrays.fill(pad, (byte) 'x');
    String padStr = new String(pad, StandardCharsets.US_ASCII);
    for (int i = 0; i < count; i++) {
      byte[] payload = (prefix + i + padStr).getBytes(StandardCharsets.UTF_8);
      row.set(0, new BytesRefWritable(payload));
      writer.append(row);
    }
    writer.close();
  }

  private int countRows(FileSplit split) throws IOException {
    RCFileRecordReader<LongWritable, BytesRefArrayWritable> reader =
        new RCFileRecordReader<LongWritable, BytesRefArrayWritable>(conf, split);
    LongWritable key = reader.createKey();
    BytesRefArrayWritable value = reader.createValue();
    int rows = 0;
    try {
      while (reader.next(key, value)) {
        rows++;
      }
    } finally {
      reader.close();
    }
    return rows;
  }
}

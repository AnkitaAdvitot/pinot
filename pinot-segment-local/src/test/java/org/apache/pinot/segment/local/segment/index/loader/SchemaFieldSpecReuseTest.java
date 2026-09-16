/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Exercises table FieldSpec sharing through real segment preprocessing and query-serving index readers.
public class SchemaFieldSpecReuseTest {
  private File _tempDir;
  private File _segmentDir;
  private TableConfig _tableConfig;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("SchemaFieldSpecReuseTest").toFile();
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("schemaReuse")
        .setSegmentVersion("v3").setNullHandlingEnabled(true).build();
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      GenericRow row = new GenericRow();
      row.putValue("id", i);
      row.putValue("tags", new String[]{"x", "y"});
      row.putValue("score", i + 0.5);
      row.putValue("ts", 1000L + i);
      if (i == 0) {
        row.putDefaultNullValue("id", -1);
      }
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, newSchema());
    config.setOutDir(_tempDir.getAbsolutePath());
    config.setSegmentName("segment");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segmentDir = driver.getOutputDirectory();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testSharedSchemaAcrossConcurrentLoads()
      throws Exception {
    Schema schema = newSchema();
    List<ImmutableSegment> segments = new ArrayList<>();
    try {
      try (var executor = Executors.newFixedThreadPool(4)) {
        List<Future<ImmutableSegment>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
          futures.add(executor.submit(() -> ImmutableSegmentLoader.load(_segmentDir,
              new IndexLoadingConfig(_tableConfig, schema), false)));
        }
        for (Future<ImmutableSegment> future : futures) {
          segments.add(future.get());
        }
      }
      for (ImmutableSegment segment : segments) {
        assertNotSame(segment.getSegmentMetadata().getSchema(), schema);
        for (FieldSpec spec : schema.getAllFieldSpecs()) {
          assertSame(segment.getSegmentMetadata().getColumnMetadataFor(spec.getName()).getFieldSpec(), spec);
          assertSame(segment.getSegmentMetadata().getSchema().getFieldSpecFor(spec.getName()), spec);
        }
        assertValues(segment);
      }
      assertNotSame(segments.get(0).getSegmentMetadata().getSchema(), segments.get(1).getSegmentMetadata().getSchema());
      assertEquals(schema.size(), 4, "Virtual columns must stay in each segment's own schema");
    } finally {
      for (ImmutableSegment segment : segments) {
        segment.destroy();
      }
    }
  }

  @Test
  public void testPhysicalDefaultMismatchAfterPreprocessing()
      throws Exception {
    Schema schema = newSchema();
    schema.getFieldSpecFor("id").setDefaultNullValue(-2);
    ImmutableSegment segment = ImmutableSegmentLoader.load(_segmentDir, new IndexLoadingConfig(_tableConfig, schema));
    try {
      FieldSpec stored = segment.getSegmentMetadata().getColumnMetadataFor("id").getFieldSpec();
      assertFalse(segment.getSegmentMetadata().getColumnMetadataFor("id").isAutoGenerated());
      assertEquals(stored.getDefaultNullValue(), -1);
      assertNotSame(stored, schema.getFieldSpecFor("id"));
      assertTrue(segment.getDataSource("id").getDictionary().indexOf((int) stored.getDefaultNullValue()) >= 0);
      assertEquals(segment.getDataSource("id").getDictionary().indexOf(-2), -1);
      assertValues(segment);
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testAutogeneratedDefaultIsRebuiltBeforeReuse()
      throws Exception {
    Schema original = newSchema();
    original.addField(new DimensionFieldSpec("added", DataType.INT, true, -10));
    ImmutableSegment first = ImmutableSegmentLoader.load(_segmentDir, new IndexLoadingConfig(_tableConfig, original));
    try {
      assertTrue(first.getSegmentMetadata().getColumnMetadataFor("added").isAutoGenerated());
      assertSame(first.getSegmentMetadata().getColumnMetadataFor("added").getFieldSpec(),
          original.getFieldSpecFor("added"));
      assertDefaultColumn(first, -10);
    } finally {
      first.destroy();
    }
    Schema updated = SchemaSerDeUtils.fromZNRecord(SchemaSerDeUtils.toZNRecord(original));
    updated.getFieldSpecFor("added").setDefaultNullValue(-20);
    ImmutableSegment second = ImmutableSegmentLoader.load(_segmentDir, new IndexLoadingConfig(_tableConfig, updated));
    try {
      assertSame(second.getSegmentMetadata().getColumnMetadataFor("added").getFieldSpec(),
          updated.getFieldSpecFor("added"));
      assertDefaultColumn(second, -20);
      assertEquals(original.getFieldSpecFor("added").getDefaultNullValue(), -10);
      assertValues(second);
    } finally {
      second.destroy();
    }
  }

  @Test
  public void testReadWithoutTableSchema()
      throws Exception {
    ImmutableSegment segment = ImmutableSegmentLoader.load(_segmentDir, ReadMode.mmap);
    try {
      assertValues(segment);
    } finally {
      segment.destroy();
    }
  }

  private static Schema newSchema() {
    return new Schema.SchemaBuilder().setSchemaName("schemaReuse")
        .addSingleValueDimension("id", DataType.INT, -1).addMultiValueDimension("tags", DataType.STRING)
        .addMetric("score", DataType.DOUBLE)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  }

  private static void assertDefaultColumn(ImmutableSegment segment, int expected)
      throws Exception {
    try (PinotSegmentColumnReader reader = new PinotSegmentColumnReader(segment, "added")) {
      for (int i = 0; i < 4; i++) {
        assertEquals(reader.getValue(i), expected);
      }
    }
  }

  private static void assertValues(ImmutableSegment segment)
      throws Exception {
    try (PinotSegmentColumnReader ids = new PinotSegmentColumnReader(segment, "id");
        PinotSegmentColumnReader tags = new PinotSegmentColumnReader(segment, "tags");
        PinotSegmentColumnReader scores = new PinotSegmentColumnReader(segment, "score");
        PinotSegmentColumnReader times = new PinotSegmentColumnReader(segment, "ts")) {
      for (int i = 0; i < 4; i++) {
        assertEquals(ids.getValue(i), i == 0 ? -1 : i);
        assertEquals((Object[]) tags.getValue(i), new String[]{"x", "y"});
        assertEquals(scores.getValue(i), i + 0.5);
        assertEquals(times.getValue(i), 1000L + i);
      }
    }
    assertTrue(segment.getDataSource("id").getNullValueVector().getNullBitmap().contains(0));
  }
}

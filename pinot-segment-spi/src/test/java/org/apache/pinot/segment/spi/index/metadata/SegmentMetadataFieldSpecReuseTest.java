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
package org.apache.pinot.segment.spi.index.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;


/// Verifies final-load field-spec reuse without changing segment metadata, schema order, or fallback values.
@SuppressWarnings("deprecation") // Verify reuse preserves legacy TIME columns.
public class SegmentMetadataFieldSpecReuseTest {
  @Test
  public void reusesEqualSpecsWithoutChangingMetadataOrSchemaOrder()
      throws Exception {
    List<FieldSpec> fields = List.of(new DimensionFieldSpec(new String("z"), DataType.INT, true),
        new DimensionFieldSpec(new String("a"), DataType.INT, true),
        new MetricFieldSpec(new String("count"), DataType.LONG),
        new DateTimeFieldSpec(new String("date"), DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"),
        new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, new String("time"))));
    SegmentMetadataImpl metadata = metadata(10, fields);
    Schema tableSchema = schema(fields);
    Schema segmentSchema = metadata.getSchema();
    FieldSpec firstDimension = segmentSchema.getDimensionFieldSpecs().get(0);
    segmentSchema.removeField(firstDimension.getName());
    segmentSchema.addField(firstDimension);
    segmentSchema.setSchemaName("segment-schema");
    segmentSchema.setEnableColumnBasedNullHandling(true);
    segmentSchema.setPrimaryKeyColumns(List.of("z"));
    List<String> dimensionOrder = new ArrayList<>(segmentSchema.getDimensionNames());
    String metadataJson = JsonUtils.objectToString(metadata);
    String schemaJson = segmentSchema.toSingleLineJsonString();
    String tableJson = tableSchema.toSingleLineJsonString();
    Map<String, ColumnMetadata> originalColumns = Map.copyOf(metadata.getColumnMetadataMap());

    metadata.reuseFieldSpecs(tableSchema);

    assertSame(metadata.getSchema(), segmentSchema);
    assertEquals(segmentSchema.getDimensionNames(), dimensionOrder);
    assertEquals(segmentSchema.toSingleLineJsonString(), schemaJson);
    assertEquals(JsonUtils.objectToString(metadata), metadataJson);
    assertEquals(tableSchema.toSingleLineJsonString(), tableJson);
    for (FieldSpec field : fields) {
      ColumnMetadata column = metadata.getColumnMetadataFor(field.getName());
      assertSame(column, originalColumns.get(field.getName()));
      assertSame(column.getFieldSpec(), field);
      assertSame(segmentSchema.getFieldSpecFor(field.getName()), field);
      assertSame(metadata.getColumnMetadataMap().ceilingKey(field.getName()), field.getName());
      assertSame(segmentSchema.getFieldSpecMap().ceilingKey(field.getName()), field.getName());
    }
    assertSame(metadata.getTimeColumn(), tableSchema.getTimeFieldSpec().getName());

    metadata.reuseFieldSpecs(tableSchema);
    assertEquals(segmentSchema.toSingleLineJsonString(), schemaJson);
    assertEquals(JsonUtils.objectToString(metadata), metadataJson);
  }

  @Test
  public void retainsMissingAndEvolvedSpecs()
      throws Exception {
    List<FieldSpec> fields = List.of(new DimensionFieldSpec("missing", DataType.INT, true),
        new DimensionFieldSpec("default", DataType.INT, true, -1),
        new DimensionFieldSpec("type", DataType.INT, true),
        new DimensionFieldSpec("multiValue", DataType.INT, false),
        new DimensionFieldSpec("notNull", DataType.INT, true),
        new DimensionFieldSpec("fieldType", DataType.INT, true),
        new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "time")));
    SegmentMetadataImpl metadata = metadata(10, fields);
    DimensionFieldSpec notNull = new DimensionFieldSpec("notNull", DataType.INT, true);
    notNull.setNotNull(true);
    Schema tableSchema = schema(List.of(new DimensionFieldSpec("default", DataType.INT, true, -2),
        new DimensionFieldSpec("type", DataType.LONG, true),
        new DimensionFieldSpec("multiValue", DataType.INT, true), notNull,
        new MetricFieldSpec("fieldType", DataType.INT),
        new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "time"))));
    Map<String, FieldSpec> before = Map.copyOf(metadata.getSchema().getFieldSpecMap());
    String json = JsonUtils.objectToString(metadata);

    metadata.reuseFieldSpecs(tableSchema);

    before.forEach((name, field) -> {
      assertSame(metadata.getColumnMetadataFor(name).getFieldSpec(), field);
      assertSame(metadata.getSchema().getFieldSpecFor(name), field);
    });
    assertEquals(JsonUtils.objectToString(metadata), json);
    assertEquals(metadata.getColumnMetadataFor("default").getFieldSpec().getDefaultNullValue(), -1);
  }

  @Test
  public void skipsComplexSpecsAndKeepsUnmatchedInternedFallbacks()
      throws Exception {
    ComplexFieldSpec complex = new ComplexFieldSpec("nested", DataType.OPEN_STRUCT, true,
        Map.of("child", new DimensionFieldSpec("nested$$child", DataType.INT, true)));
    List<FieldSpec> fields = List.of(complex, new DimensionFieldSpec("matched", DataType.INT, true),
        new DimensionFieldSpec("missing", DataType.INT, true));
    SegmentMetadataImpl first = metadata(10, fields);
    SegmentMetadataImpl second = metadata(10, fields);
    FieldSpec originalComplex = first.getColumnMetadataFor("nested").getFieldSpec();
    ComplexFieldSpec evolved = new ComplexFieldSpec("nested", DataType.OPEN_STRUCT, true,
        Map.of("other", new DimensionFieldSpec("nested$$other", DataType.LONG, true)));
    assertEquals(originalComplex, evolved, "Complex equality does not compare children");
    DimensionFieldSpec matched = new DimensionFieldSpec("matched", DataType.INT, true);
    Schema tableSchema = schema(List.of(evolved, matched));
    assertNotSame(first.getColumnMetadataFor("matched").getFieldSpec(), matched);

    first.reuseFieldSpecs(tableSchema);
    second.reuseFieldSpecs(tableSchema);

    assertSame(first.getColumnMetadataFor("nested").getFieldSpec(), originalComplex);
    assertSame(first.getSchema().getFieldSpecFor("nested"), originalComplex);
    assertEquals(((ComplexFieldSpec) originalComplex).getChildFieldSpecs().keySet(),
        complex.getChildFieldSpecs().keySet());
    assertSame(first.getColumnMetadataFor("matched").getFieldSpec(), matched);
    assertSame(second.getColumnMetadataFor("matched").getFieldSpec(), matched);
    assertSame(first.getColumnMetadataFor("missing").getFieldSpec(),
        second.getColumnMetadataFor("missing").getFieldSpec());
  }

  @Test
  public void skipsEmptyAndConsumingMetadataAndAbsentSchema()
      throws Exception {
    Schema tableSchema = schema(List.of(new DimensionFieldSpec("empty", DataType.INT, true)));
    SegmentMetadataImpl empty = metadata(0, new ArrayList<>(tableSchema.getAllFieldSpecs()));
    FieldSpec original = empty.getColumnMetadataFor("empty").getFieldSpec();
    empty.reuseFieldSpecs(tableSchema);
    assertSame(empty.getColumnMetadataFor("empty").getFieldSpec(), original);

    SegmentMetadataImpl consuming = new SegmentMetadataImpl("table", "consuming", tableSchema, 0);
    consuming.reuseFieldSpecs(tableSchema);
    assertSame(consuming.getSchema(), tableSchema);

    SegmentMetadataImpl regular = metadata(10, new ArrayList<>(tableSchema.getAllFieldSpecs()));
    FieldSpec regularSpec = regular.getColumnMetadataFor("empty").getFieldSpec();
    regular.reuseFieldSpecs(null);
    assertSame(regular.getColumnMetadataFor("empty").getFieldSpec(), regularSpec);
  }

  @Test
  public void doesNotMatchMaterializedChildAgainstTopLevelColumn()
      throws Exception {
    SegmentMetadataImpl metadata = metadata(10, List.of(new DimensionFieldSpec("cpu", DataType.INT, true, -1)));
    FieldSpec topLevel = metadata.getColumnMetadataFor("cpu").getFieldSpec();
    DimensionFieldSpec child = new DimensionFieldSpec("cpu", DataType.INT, true, 0);
    ColumnMetadataImpl childMetadata = ColumnMetadataImpl.builder().setFieldSpec(child).setTotalDocs(10).build();
    metadata.getColumnMetadataMap().put("metrics$cpu", childMetadata);
    Schema tableSchema = schema(List.of(new DimensionFieldSpec("cpu", DataType.INT, true, 0)));

    metadata.reuseFieldSpecs(tableSchema);

    assertSame(metadata.getColumnMetadataFor("cpu").getFieldSpec(), topLevel);
    assertSame(metadata.getSchema().getFieldSpecFor("cpu"), topLevel);
    assertSame(metadata.getColumnMetadataFor("metrics$cpu"), childMetadata);
    assertSame(childMetadata.getFieldSpec(), child);
    assertEquals(topLevel.getDefaultNullValue(), -1);
  }

  private static Schema schema(List<FieldSpec> fields) {
    Schema schema = new Schema();
    fields.forEach(schema::addField);
    return schema;
  }

  private static SegmentMetadataImpl metadata(int totalDocs, List<FieldSpec> fields)
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(Segment.SEGMENT_NAME, "reuse-test");
    properties.setProperty(Segment.SEGMENT_TOTAL_DOCS, Integer.toString(totalDocs));
    Map<FieldType, String> lists = Map.of(FieldType.DIMENSION, Segment.DIMENSIONS, FieldType.METRIC, Segment.METRICS,
        FieldType.TIME, Segment.TIME_COLUMN_NAME, FieldType.DATE_TIME, Segment.DATETIME_COLUMNS,
        FieldType.COMPLEX, Segment.COMPLEX_COLUMNS);
    lists.forEach((type, key) -> {
      String names = fields.stream().filter(field -> field.getFieldType() == type).map(FieldSpec::getName)
          .collect(Collectors.joining(","));
      if (!names.isEmpty()) {
        properties.setProperty(key, names);
      }
    });
    fields.forEach(field -> writeField(properties, field.getName(), field));
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    properties.store(stream, null);
    byte[] creationMetadata = ByteBuffer.allocate(16).putLong(1).putLong(2).array();
    return new SegmentMetadataImpl(new ByteArrayInputStream(stream.toByteArray()),
        new ByteArrayInputStream(creationMetadata));
  }

  private static void writeField(Properties properties, String column, FieldSpec field) {
    properties.setProperty(Column.getKeyFor(column, Column.COLUMN_NAME), field.getName());
    properties.setProperty(Column.getKeyFor(column, Column.COLUMN_TYPE), field.getFieldType().name());
    properties.setProperty(Column.getKeyFor(column, Column.DATA_TYPE), field.getDataType().name());
    properties.setProperty(Column.getKeyFor(column, Column.IS_SINGLE_VALUED),
        Boolean.toString(field.isSingleValueField()));
    properties.setProperty(Column.getKeyFor(column, Column.CARDINALITY), "1");
    if (field instanceof ComplexFieldSpec) {
      Map<String, FieldSpec> children = ((ComplexFieldSpec) field).getChildFieldSpecs();
      properties.setProperty(Column.getKeyFor(column, Column.COMPLEX_CHILD_FIELD_NAMES),
          String.join(",", children.keySet()));
      children.forEach((name, child) -> writeField(properties, ComplexFieldSpec.getFullChildName(column, name), child));
    } else {
      properties.setProperty(Column.getKeyFor(column, Column.DEFAULT_NULL_VALUE), field.getDefaultNullValueString());
    }
    if (field instanceof DateTimeFieldSpec) {
      properties.setProperty(Column.getKeyFor(column, Column.DATETIME_FORMAT), ((DateTimeFieldSpec) field).getFormat());
      properties.setProperty(Column.getKeyFor(column, Column.DATETIME_GRANULARITY),
          ((DateTimeFieldSpec) field).getGranularity());
    }
    if (field instanceof TimeFieldSpec) {
      properties.setProperty(Segment.TIME_UNIT,
          ((TimeFieldSpec) field).getIncomingGranularitySpec().getTimeType().name());
    }
  }
}

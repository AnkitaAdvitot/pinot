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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


/// Verifies parse-time reuse preserves stored definitions and serialized metadata, including fallback paths.
@SuppressWarnings("deprecation")
public class SegmentMetadataFieldSpecReuseTest {
  @Test
  public void reusesScalarSpecsWithoutChangingMetadataOrSchemaOrder() throws Exception {
    List<FieldSpec> fields = List.of(new DimensionFieldSpec(new String("z"), DataType.INT, true),
        new DimensionFieldSpec(new String("a"), DataType.STRING, false),
        new MetricFieldSpec("count", DataType.LONG),
        new DateTimeFieldSpec("date", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"),
        new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "time")));
    Schema table = schema(fields);
    SegmentMetadataImpl baseline = metadata(10, fields);
    SegmentMetadataImpl shared = metadata(10, fields, new SegmentSchemaContext(table), Map.of());
    assertEquals(shared.toJson(null), baseline.toJson(null));
    assertEquals(shared.getSchema().toSingleLineJsonString(), baseline.getSchema().toSingleLineJsonString());
    assertNotSame(shared.getSchema(), table);
    for (FieldSpec field : fields) {
      FieldSpec actual = shared.getColumnMetadataFor(field.getName()).getFieldSpec();
      assertSame(actual, shared.getSchema().getFieldSpecFor(field.getName()));
      assertSame(actual, field instanceof TimeFieldSpec
          ? baseline.getColumnMetadataFor(field.getName()).getFieldSpec() : field);
    }
  }

  @Test
  public void projectsLogicalOnlyAttributesOnceWithoutChangingMetadata() throws Exception {
    DimensionFieldSpec stored = new DimensionFieldSpec("id", DataType.STRING, true);
    DimensionFieldSpec logical = new DimensionFieldSpec("id", DataType.STRING, true, 512, null);
    logical.setNotNull(true);
    logical.setDescription("table-only description");
    logical.setAliases(List.of("old_id"));
    SegmentSchemaContext context = new SegmentSchemaContext(schema(List.of(logical)));
    SegmentMetadataImpl first = metadata(10, List.of(stored), context, Map.of());
    SegmentMetadataImpl second = metadata(10, List.of(stored), context, Map.of());
    FieldSpec projected = first.getColumnMetadataFor("id").getFieldSpec();
    assertSame(second.getColumnMetadataFor("id").getFieldSpec(), projected);
    assertNotSame(projected, logical);
    assertEquals(first.toJson(null), metadata(10, List.of(stored)).toJson(null));
    assertEquals(logical.getDescription(), "table-only description");
    assertEquals(logical.getAliases(), List.of("old_id"));
  }

  @Test
  public void retainsHistoricalDefaultsTypesAndMissingFields() throws Exception {
    List<FieldSpec> stored = List.of(new DimensionFieldSpec("default", DataType.INT, true, -1),
        new DimensionFieldSpec("type", DataType.INT, true),
        new DimensionFieldSpec("mv", DataType.INT, false),
        new DimensionFieldSpec("missing", DataType.INT, true));
    Schema table = schema(List.of(new DimensionFieldSpec("default", DataType.INT, true, -2),
        new DimensionFieldSpec("type", DataType.LONG, true), new DimensionFieldSpec("mv", DataType.INT, true)));
    SegmentMetadataImpl baseline = metadata(10, stored);
    SegmentMetadataImpl actual = metadata(10, stored, new SegmentSchemaContext(table), Map.of());
    assertEquals(actual.toJson(null), baseline.toJson(null));
    for (FieldSpec field : stored) {
      assertSame(actual.getColumnMetadataFor(field.getName()).getFieldSpec(),
          baseline.getColumnMetadataFor(field.getName()).getFieldSpec());
    }
    assertEquals(actual.getColumnMetadataFor("default").getFieldSpec().getDefaultNullValue(), -1);
  }

  @Test
  public void preservesComplexChildrenAndInternedFallbacks() throws Exception {
    ComplexFieldSpec stored = new ComplexFieldSpec("nested", DataType.OPEN_STRUCT, true,
        Map.of("child", new DimensionFieldSpec("nested$$child", DataType.INT, true)));
    ComplexFieldSpec logical = new ComplexFieldSpec("nested", DataType.OPEN_STRUCT, true,
        Map.of("other", new DimensionFieldSpec("nested$$other", DataType.LONG, true)));
    assertEquals(stored, logical, "Complex equality does not compare children");
    SegmentMetadataImpl actual = metadata(10, List.of(stored), new SegmentSchemaContext(schema(List.of(logical))),
        Map.of());
    assertEquals(actual.toJson(null), metadata(10, List.of(stored)).toJson(null));
    ComplexFieldSpec actualSpec = (ComplexFieldSpec) actual.getColumnMetadataFor("nested").getFieldSpec();
    assertEquals(actualSpec.getChildFieldSpecs().keySet(),
        stored.getChildFieldSpecs().keySet());
  }

  @Test
  public void reusesEmptySegmentDefinitionsAndKeepsConsumingSchema() throws Exception {
    DimensionFieldSpec field = new DimensionFieldSpec("empty", DataType.INT, true);
    Schema table = schema(List.of(field));
    SegmentMetadataImpl empty = metadata(0, List.of(field), new SegmentSchemaContext(table), Map.of());
    assertSame(empty.getColumnMetadataFor("empty").getFieldSpec(), field);
    assertEquals(empty.toJson(null), metadata(0, List.of(field)).toJson(null));
    assertSame(new SegmentMetadataImpl("table", "consuming", table, 0).getSchema(), table);
  }

  @Test
  public void doesNotMatchMaterializedChildToTopLevelDefinition() throws Exception {
    DimensionFieldSpec field = new DimensionFieldSpec("cpu", DataType.INT, true);
    SegmentSchemaContext context = new SegmentSchemaContext(schema(List.of(field)));
    SegmentMetadataImpl child = metadata(10, List.of(field), context,
        Map.of(Column.getKeyFor("cpu", Column.PARENT_COLUMN), "metrics"));
    assertNotSame(child.getColumnMetadataFor("cpu").getFieldSpec(), field);
    assertEquals(child.getColumnMetadataFor("cpu").getFieldSpec(), field);
  }

  @Test
  public void preservesStorageKeyAndLogicalNameDistinction() throws Exception {
    DimensionFieldSpec stored = new DimensionFieldSpec("physical", DataType.INT, true);
    DimensionFieldSpec logical = new DimensionFieldSpec("logical", DataType.INT, true);
    Map<String, String> overrides = Map.of(Column.getKeyFor("physical", Column.COLUMN_NAME), "logical");
    SegmentMetadataImpl baseline = metadata(10, List.of(stored), null, overrides);
    SegmentMetadataImpl actual = metadata(10, List.of(stored),
        new SegmentSchemaContext(schema(List.of(logical))), overrides);
    assertEquals(actual.toJson(null), baseline.toJson(null));
    assertEquals(actual.getColumnMetadataMap().keySet(), baseline.getColumnMetadataMap().keySet());
    assertEquals(actual.getSchema().getColumnNames(), baseline.getSchema().getColumnNames());
    assertNotSame(actual.getColumnMetadataFor("physical").getFieldSpec(), logical);
  }

  @Test
  public void preservesExplicitLengthSettingsAndDatetimeChanges() throws Exception {
    DimensionFieldSpec field = new DimensionFieldSpec("str", DataType.STRING, true);
    DateTimeFieldSpec date = new DateTimeFieldSpec("date", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    List<FieldSpec> fields = List.of(field, date);
    Map<String, String> overrides = Map.of(Column.getKeyFor("str", Column.SCHEMA_MAX_LENGTH), "512",
        Column.getKeyFor("date", Column.DATETIME_GRANULARITY), "1:HOURS");
    SegmentMetadataImpl baseline = metadata(10, fields, null, overrides);
    SegmentMetadataImpl actual = metadata(10, fields, new SegmentSchemaContext(schema(fields)), overrides);
    assertEquals(actual.toJson(null), baseline.toJson(null));
    assertNotSame(actual.getColumnMetadataFor("str").getFieldSpec(), field);
    assertNotSame(actual.getColumnMetadataFor("date").getFieldSpec(), date);
  }

  @Test
  public void doesNotSkipValidationOnSharingPath() {
    DimensionFieldSpec field = new DimensionFieldSpec("id", DataType.INT, true);
    SegmentSchemaContext context = new SegmentSchemaContext(schema(List.of(field)));
    Map<String, String> invalid = Map.of(Column.getKeyFor("id", Column.SCHEMA_MAX_LENGTH_EXCEED_STRATEGY), "INVALID");
    assertThrows(IllegalArgumentException.class, () -> metadata(10, List.of(field), null, invalid));
    assertThrows(IllegalArgumentException.class, () -> metadata(10, List.of(field), context, invalid));
  }

  @Test
  public void preservesCustomDefaultsAcrossStoredTypes() throws Exception {
    List<FieldSpec> fields = List.of(
        new DimensionFieldSpec("int", DataType.INT, true, -123),
        new DimensionFieldSpec("long", DataType.LONG, true, -123L),
        new DimensionFieldSpec("float", DataType.FLOAT, true, -0.0f),
        new DimensionFieldSpec("double", DataType.DOUBLE, true, -0.0d),
        new DimensionFieldSpec("decimal", DataType.BIG_DECIMAL, true, new BigDecimal("1.00")),
        new DimensionFieldSpec("boolean", DataType.BOOLEAN, true, true),
        new DimensionFieldSpec("timestamp", DataType.TIMESTAMP, true),
        new DimensionFieldSpec("string", DataType.STRING, true, "custom value"),
        new DimensionFieldSpec("json", DataType.JSON, true, "{}"),
        new DimensionFieldSpec("bytes", DataType.BYTES, true, new byte[]{1, 2}),
        new DimensionFieldSpec("uuid", DataType.UUID, true));
    SegmentMetadataImpl baseline = metadata(10, fields);
    SegmentMetadataImpl shared = metadata(10, fields, new SegmentSchemaContext(schema(fields)), Map.of());
    assertEquals(shared.toJson(null), baseline.toJson(null));
    for (FieldSpec field : fields) {
      assertSame(shared.getColumnMetadataFor(field.getName()).getFieldSpec(), field);
    }
  }

  private static Schema schema(List<FieldSpec> fields) {
    Schema schema = new Schema();
    fields.forEach(schema::addField);
    return schema;
  }

  private static SegmentMetadataImpl metadata(int totalDocs, List<FieldSpec> fields) throws Exception {
    return metadata(totalDocs, fields, null, Map.of());
  }

  private static SegmentMetadataImpl metadata(int totalDocs, List<FieldSpec> fields,
      SegmentSchemaContext context, Map<String, String> overrides) throws Exception {
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
    overrides.forEach(properties::setProperty);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    properties.store(stream, null);
    byte[] creationMetadata = ByteBuffer.allocate(16).putLong(1).putLong(2).array();
    return new SegmentMetadataImpl(new ByteArrayInputStream(stream.toByteArray()),
        new ByteArrayInputStream(creationMetadata), context);
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
    if (field.getNonDefaultMaxLength() != null) {
      properties.setProperty(Column.getKeyFor(column, Column.SCHEMA_MAX_LENGTH),
          field.getNonDefaultMaxLength().toString());
    }
    if (field.getNonDefaultMaxLengthExceedStrategy() != null) {
      properties.setProperty(Column.getKeyFor(column, Column.SCHEMA_MAX_LENGTH_EXCEED_STRATEGY),
          field.getNonDefaultMaxLengthExceedStrategy().name());
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

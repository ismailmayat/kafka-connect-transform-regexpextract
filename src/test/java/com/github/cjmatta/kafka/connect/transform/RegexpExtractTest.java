package com.github.cjmatta.kafka.connect.transform;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

public class RegexpExtractTest {
  private final RegexpExtract<SinkRecord> xform = new RegexpExtract.Value<SinkRecord>();

  @After
  public void tearDown() {
    xform.close();
  }

  @Test
  public void schemaless() {
    final Map<String, String> props = new HashMap<>();
    props.put("source.field.name", "source");
    props.put("destination.field.name", "destination");
    props.put("pattern", "C.t");

    xform.configure(props);

    final Map<String, Object> value = new HashMap<>();
    value.put("source", "Cat Cut Cot");

    final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertEquals("Cat", updatedValue.get("destination"));
  }

  @Test
  public void schemalessInsensitive() {
    final Map<String, String> props = new HashMap<>();
    props.put("source.field.name", "source");
    props.put("destination.field.name", "destination");
    props.put("pattern", "c.t");
    props.put("occurrence", "1");
    props.put("case.sensitive", "false");

    xform.configure(props);

    final Map<String, Object> value = new HashMap<>();
    value.put("source", "Cat Cut Cot");

    final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertEquals("Cat", updatedValue.get("destination"));
  }

  @Test
  public void schemalessWithKey(){
    final Map<String, String> props = new HashMap<>();
    props.put("source.field.name", "source");
    props.put("useKey","true");
    props.put("destination.field.name", "destination");
    props.put("pattern", "(?<=primary_key=)\\d+");
    props.put("occurrence", "1");
    props.put("case.sensitive", "false");

    xform.configure(props);

    final Map<String, Object> value = new HashMap<>();
    value.put("source", "whatever");

    final SinkRecord record = new SinkRecord("test", 0, null, "Struct{table=XE.MYUSER.AGDA,column=MYCLOB,primary_key=3}", null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertEquals("3", updatedValue.get("destination"));
  }

  @Test
  public void schemalessNoMatch() {
    final Map<String, String> props = new HashMap<>();
    props.put("source.field.name", "source");
    props.put("destination.field.name", "destination");
    props.put("pattern", "BadPattern");
    props.put("occurrence", "1");

    xform.configure(props);

    final Map<String, Object> value = new HashMap<>();
    value.put("source", "Cat Cut Cot");

    final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertEquals(null, updatedValue.get("destination"));
  }


  @Test
  public void schemalessOccurrence() {
    final Map<String, String> props = new HashMap<>();
    props.put("source.field.name", "source");
    props.put("destination.field.name", "destination");
    props.put("pattern", "C.t");
    props.put("occurrence", "2");

    xform.configure(props);

    final Map<String, Object> value = new HashMap<>();
    value.put("source", "Cat Cut Cot");

    final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertEquals("Cut", updatedValue.get("destination"));
  }

  @Test
  public void withSchema() {
    final Map<String, String> props = new HashMap<>();
    props.put("source.field.name", "source");
    props.put("destination.field.name", "destination");
    props.put("pattern", "C.t");

    xform.configure(props);

    final Schema schema = SchemaBuilder.struct()
            .field("source", Schema.STRING_SCHEMA)
            .build();

    final Struct value = new Struct(schema);
    value.put("source", "Cat Cut Cot");

    final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Struct updatedValue = (Struct) transformedRecord.value();
    assertEquals("Cat", updatedValue.get("destination"));
  }

}
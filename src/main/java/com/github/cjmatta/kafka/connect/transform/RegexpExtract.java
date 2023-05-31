package com.github.cjmatta.kafka.connect.transform;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class RegexpExtract<R extends ConnectRecord<R>> implements Transformation<R> {
  public static final String OVERVIEW_DOC =
    "Extract a substring from a field using a regular expression";

  private interface ConfigName {
    String SOURCE_FIELD_NAME = "source.field.name";
    String PATTERN = "pattern";
    String DESTINATION_FIELD_NAME = "destination.field.name";
    String MATCH_OCCURRENCE = "occurrence";
    String CASE_SENSITIVE = "case.sensitive";

    String USE_KEY = "useKey";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
          .define(ConfigName.SOURCE_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
      "Field to search with regexp pattern")
          .define(ConfigName.DESTINATION_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
      "Field to store pattern's match")
          .define(ConfigName.PATTERN, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
      "Regular Expression to match")
          .define(ConfigName.MATCH_OCCURRENCE, ConfigDef.Type.INT, 1, ConfigDef.Importance.MEDIUM,
      "Match occurrence, defaults to the first match")
          .define(ConfigName.CASE_SENSITIVE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                  "Match case sensitive, defaults to true")
          .define(ConfigName.USE_KEY,ConfigDef.Type.BOOLEAN,false, ConfigDef.Importance.MEDIUM,"Use key as source, defaults to false")
          ;

  private static final String PURPOSE = "extract substring using a regular expression";

  private String sourceFieldName;
  private String destinationFieldName;
  private String pattern;
  private int occurrence;
  private Boolean caseSensitive;
  private Cache<Schema, Schema> schemaUpdateCache;

  private Boolean useKey = false;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    sourceFieldName = config.getString(ConfigName.SOURCE_FIELD_NAME);
    destinationFieldName = config.getString(ConfigName.DESTINATION_FIELD_NAME);
    pattern = config.getString(ConfigName.PATTERN);
    occurrence = config.getInt(ConfigName.MATCH_OCCURRENCE);
    caseSensitive = config.getBoolean(ConfigName.CASE_SENSITIVE);
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    useKey = config.getBoolean(ConfigName.USE_KEY);
  }

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

    final Map<String, Object> updatedValue = new HashMap<>(value);

    String matched = "";

    if(useKey){
      matched = getRegexpMatch(record.key().toString(),pattern);
    }
    else{
      matched = getRegexpMatch(value.get(sourceFieldName).toString(), pattern);
    }

    updatedValue.put(destinationFieldName, matched);

    return newRecord(record, null, updatedValue);
  }


  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if(updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema());
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      updatedValue.put(field.name(), value.get(field));
    }

    String matched = getRegexpMatch(value.get(sourceFieldName).toString(), pattern);
    updatedValue.put(destinationFieldName, matched);

    return newRecord(record, updatedSchema, updatedValue);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private String getRegexpMatch(String input, String pattern) {
    Pattern p;
    int occurrenceCount = 1;

    if (caseSensitive) {
      p = Pattern.compile(pattern);
    } else {
      p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
    }
    Matcher matcher = p.matcher(input);

    try {
      matcher.find();
      if (occurrence == 1) {
        return matcher.group();
      } else {
        while(matcher.find()) {
          occurrenceCount += 1;
          if (occurrenceCount == occurrence) {
            return matcher.group();
          }
        }
      }
    } catch (IllegalStateException e) {
      return null;
    }
    return null;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(destinationFieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }


  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends RegexpExtract<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends RegexpExtract<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }


}


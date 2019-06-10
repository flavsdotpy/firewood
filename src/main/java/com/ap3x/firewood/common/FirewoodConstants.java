package com.ap3x.firewood.common;

import org.apache.spark.sql.types.DataType;
import org.spark_project.guava.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.*;

public interface FirewoodConstants {

    public static final String COMMON_DEMILITER = "|";

    public static final Map<String, String> SOURCE_TYPE_KEY_MAP = ImmutableMap.of(
            "file","file_path",
            "enrichment","job_name",
            "db","table_name"
    );

    public static final Map<String, String> SOURCE_TRACKING_KEY_MAP = ImmutableMap.of(
            "file","exported_at"
    );

    Map<String, DataType> DATA_TYPE_MAP = new HashMap<String, DataType>() {

        private static final long serialVersionUID = -1688537081297060262L;

        {
            put("int", IntegerType);
            put("string", StringType);
            put("double", DoubleType);
            put("float", FloatType);
            put("boolean", BooleanType);
            put("datetime", DateType);
            put("long", LongType);
            put("null", NullType);
            put("timestamp", TimestampType);
        }
    };

    String BASE_QUERY = "SELECT * FROM {table} WHERE {offset_field} > {last_execution} AND {offset_field} <= {now}";

}

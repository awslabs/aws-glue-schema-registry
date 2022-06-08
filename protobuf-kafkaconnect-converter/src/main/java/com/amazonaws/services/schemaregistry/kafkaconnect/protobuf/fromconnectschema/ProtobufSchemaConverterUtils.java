/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import java.util.Calendar;
import java.util.TimeZone;

public class ProtobufSchemaConverterUtils {

    private static final String MAP_ENTRY_SUFFIX = "Entry";

    public static String getTypeName(String typeName) {
        return typeName.startsWith(".") ? typeName : "." + typeName;
    }

    public static String toMapEntryName(String s) {
        if (s.contains("_")) {
            s = LOWER_UNDERSCORE.to(UPPER_CAMEL, s);
        }
        s += MAP_ENTRY_SUFFIX;
        s = s.substring(0, 1).toUpperCase() + s.substring(1);
        return s;
    }

    public static String getSchemaSimpleName(String schemaName) {
        String[] names = schemaName.split("\\.");
        return names[names.length - 1];
    }

    public static boolean isEnumType(Schema schema) {
        return schema.type().equals(org.apache.kafka.connect.data.Schema.Type.STRING)
                && schema.parameters() != null
                && schema.parameters().containsKey(PROTOBUF_TYPE)
                && PROTOBUF_ENUM_TYPE.equals(schema.parameters().get(PROTOBUF_TYPE));
    }

    public static boolean isTimeType(Schema schema) {
        return Date.SCHEMA.name().equals(schema.name())
                || Timestamp.SCHEMA.name().equals(schema.name())
                || Time.SCHEMA.name().equals(schema.name());
    }

    public static java.util.Date convertFromGoogleDate(com.google.type.Date date) {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        cal.setLenient(false);
        cal.set(Calendar.YEAR, date.getYear());
        cal.set(Calendar.MONTH, date.getMonth() - 1); // Months start at 0, not 1
        cal.set(Calendar.DAY_OF_MONTH, date.getDay());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public static java.util.Date convertFromGoogleTime(com.google.type.TimeOfDay time) {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        //year, month and day must be hardcoded to match the Unix epoch - https://en.wikipedia.org/wiki/Unix_time
        cal.set(Calendar.YEAR, 1969);
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DAY_OF_MONTH, 32);
        cal.set(Calendar.HOUR_OF_DAY, time.getHours());
        cal.set(Calendar.MINUTE, time.getMinutes());
        cal.set(Calendar.SECOND, time.getSeconds());
        cal.set(Calendar.MILLISECOND, time.getNanos() / 1000000);
        return cal.getTime();
    }
}

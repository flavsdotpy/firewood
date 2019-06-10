package com.ap3x.firewood.helpers;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class LogHelper {

    public Row parseLog(final String logEntry){
        final String severity = getSeverity(logEntry);
        final String timestamp = getTimestamp(logEntry);
        return RowFactory.create(severity, timestamp, logEntry);
    }

    private String getSeverity(final String logEntry){
        if (logEntry.contains("FATAL"))
            return "FATAL";
        if (logEntry.contains("ERROR"))
            return "ERROR";
        if (logEntry.contains("WARN"))
            return "WARN";
        if (logEntry.contains("INFO"))
            return "INFO";
        if (logEntry.contains("DEBUG"))
            return "DEBUG";
        if (logEntry.contains("TRACE"))
            return "TRACE";
        return null;
    }

    private String getTimestamp(final String logEntry){
        Matcher matcher;
        Pattern dateTimePattern = Pattern.compile("[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2},[0-9]{1,3}");
        Pattern slashDateTimePattern = Pattern.compile("[0-9]{1,4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2},[0-9]{1,3}");
        Pattern datePattern = Pattern.compile("[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}");
        Pattern slashDatePattern = Pattern.compile("[0-9]{1,4}/[0-9]{1,2}/[0-9]{1,2}");

        matcher = dateTimePattern.matcher(logEntry);
        if (matcher.find())
            return matcher.group();

        matcher = slashDateTimePattern.matcher(logEntry);
        if (matcher.find())
            return matcher.group();

        matcher = datePattern.matcher(logEntry);
        if (matcher.find())
            return matcher.group();

        matcher = slashDatePattern.matcher(logEntry);
        if (matcher.find())
            return matcher.group();

        return null;
    }

}

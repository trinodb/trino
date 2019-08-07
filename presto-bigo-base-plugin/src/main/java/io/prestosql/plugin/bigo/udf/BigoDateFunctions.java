package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import io.airlift.log.Logger;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class BigoDateFunctions {
    private static final Logger LOG = Logger.get(BigoDateFunctions.class);
    private static final String defaultFormat = "yyyy-MM-dd HH:mm:ss";
    private static final String dateFormat = "yyyy-MM-dd";

    @Description("Returns the date that is num_days after start_date.")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateAdd(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long daysToAdd)
    {
        // create SimpleDateFormat in every call as it's not thread safe.
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);

        Calendar c = Calendar.getInstance();
        try {
            c.setTime(formatter.parse(startDate.toStringUtf8()));
        } catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        c.add(Calendar.DATE, (int)daysToAdd);

        return utf8Slice(formatter.format(c.getTime()));
    }

    @Description("Returns the date that is num_days before start_date.")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateSub(
            @SqlType(StandardTypes.VARCHAR) Slice startDate,
            @SqlType(StandardTypes.INTEGER) long daysToAdd)
    {
        return dateAdd(startDate, -daysToAdd);
    }

    @ScalarFunction("toUnixTime")
    @SqlType(StandardTypes.DOUBLE)
    public static double toUnixTime(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return timestamp / 1000.0;
    }

    @ScalarFunction("unix_timestamp")
    @SqlType(StandardTypes.DOUBLE)
    public static double unixTimestamp(@SqlType(StandardTypes.VARCHAR) Slice sliceTime)
    {
        SimpleDateFormat df = new SimpleDateFormat(defaultFormat);
        try{
            Date date = df.parse(sliceTime.toStringUtf8());
            return toUnixTime(date.getTime());
        }catch(Exception e){
            LOG.info(e.getMessage());
        }
        return 0;
    }

    @ScalarFunction("unix_timestamp")
    @SqlType(StandardTypes.DOUBLE)
    public static double unixTimestamp (@SqlType(StandardTypes.VARCHAR) Slice sliceTime, @SqlType(StandardTypes.VARCHAR) Slice sliceFormat)
    {
        SimpleDateFormat df = new SimpleDateFormat(sliceFormat.toStringUtf8());
        try{
            Date date = df.parse(sliceTime.toStringUtf8());
            return toUnixTime(date.getTime());
        }catch(Exception e){
            LOG.info(e.getMessage());
        }
        return 0;
    }

    @Description("Returns the date that is num_days after start_date.")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateDiff(
            @SqlType(StandardTypes.VARCHAR) Slice endDate,
            @SqlType(StandardTypes.VARCHAR) Slice startDate)
    {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);

        Calendar cEnd = Calendar.getInstance();
        Calendar cStart = Calendar.getInstance();
        try {
            cEnd.setTime(formatter.parse(endDate.toStringUtf8()));
            cStart.setTime(formatter.parse(startDate.toStringUtf8()));
        } catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        long diff = TimeUnit.MILLISECONDS.toDays(cEnd.getTimeInMillis()-cStart.getTimeInMillis());
        return utf8Slice(String.valueOf(diff));
    }
}

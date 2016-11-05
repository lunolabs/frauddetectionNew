package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.joda.time.DateTime;
import java.util.Date;

import java.util.List;
import java.util.ArrayList;

public class QuantilleDateTimeDetection extends DetectionBaseSpark<DateTime> {

    private Double precision = 1.0;

    public QuantilleDateTimeDetection(JavaSparkContext sc) {
        super(sc);
    }

    public QuantilleDateTimeDetection() {
        super();
    }

    public QuantilleDateTimeDetection(String master){
        super(master);
    }

    public QuantilleDateTimeDetection(String master, Double prec) {
        super();
        precision = prec;
    }

    private DateTime convertToDateTime(Date value) {
        return new DateTime(value.getTime());
    }

    private List<DateTime> convertToDateTime(List<Date> data) {
        ArrayList<DateTime> newData = new ArrayList<DateTime>();

        for(int i = 0; i < data.size(); ++i) {
            newData.add(new DateTime(data.get(i).getTime()));
        }

        return newData;
    }

    //getTime is a callback for extracting proper value from DateTime objects
    private Boolean isOKDateTime(JavaRDD<DateTime> data, DateTime value, Function<DateTime, Integer> getTime){
        JavaRDD<DateTime> dataSort = data.sortBy(getTime, true, data.getNumPartitions());
        Integer quantille25 = 0;
        try {
            quantille25 = getTime.call( dataSort.collect().get((int) dataSort.count() / 4));
        } catch (Exception e) {
            e.printStackTrace();
        }

        JavaRDD<DateTime> dataSortReverse = data.sortBy(getTime, false, data.getNumPartitions());
        Integer quantille75 = 0;
        try {
            quantille75 = getTime.call(dataSortReverse.collect().get((int) dataSortReverse.count() / 4));
        } catch (Exception e) {
            e.printStackTrace();
        }

        Integer distance = quantille75 - quantille25;
        Boolean isOK = false;
        try {
            isOK = getTime.call(value) > quantille25 - precision * distance && getTime.call(value) < quantille75 + precision * distance;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return isOK;
    }

    @Override

    public Boolean detect(List<DateTime> data, DateTime value) {

        JavaRDD<DateTime> dateTimeDB = sc.parallelize(data);

        Boolean daysOfWeek = isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getDayOfWeek();
        });

        Boolean daysOfMonth = isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getDayOfMonth();
        });

        Boolean daysOfYear = isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getDayOfYear();
        });

        Boolean weeksOfYear = isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getWeekOfWeekyear();
        });

        Boolean monthsOfYear = isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getMonthOfYear();
        });

        Boolean timeOfDay = isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getMinuteOfDay();
        });

        return daysOfWeek && daysOfMonth && daysOfYear && weeksOfYear && monthsOfYear && timeOfDay;
    }

    public Boolean detect(List<Date> data, Date value){
        return detect(convertToDateTime(data), convertToDateTime(value));
    }

    public Boolean detectDayOfWeek(List<DateTime> data, DateTime value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(data);

        return isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getDayOfWeek();
        });
    }

    public Boolean detectDayOfWeek(List<Date> data, Date value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(convertToDateTime(data));

        return isOKDateTime(dateTimeDB, convertToDateTime(value), (dt) -> {
            return dt.getDayOfWeek();
        });
    }

    public Boolean detectDayOfMonth(List<DateTime> data, DateTime value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(data);

        return isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getDayOfMonth();
        });
    }

    public Boolean detectDayOfMonth(List<Date> data, Date value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(convertToDateTime(data));

        return isOKDateTime(dateTimeDB, convertToDateTime(value), (dt) -> {
            return dt.getDayOfMonth();
        });
    }

    public Boolean detectDayOfYear(List<DateTime> data, DateTime value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(data);

        return isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getDayOfYear();
        });
    }

    public Boolean detectDayOfYear(List<Date> data, Date value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(convertToDateTime(data));

        return isOKDateTime(dateTimeDB, convertToDateTime(value), (dt) -> {
            return dt.getDayOfYear();
        });
    }

    public Boolean detectWeekOfYear(List<DateTime> data, DateTime value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(data);

        return isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getWeekOfWeekyear();
        });
    }

    public Boolean detectWeekOfYear(List<Date> data, Date value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(convertToDateTime(data));

        return isOKDateTime(dateTimeDB, convertToDateTime(value), (dt) -> {
            return dt.getWeekOfWeekyear();
        });
    }

    public Boolean detectMonthOfYear(List<DateTime> data, DateTime value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(data);

        return isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getMonthOfYear();
        });
    }

    public Boolean detectMonthOfYear(List<Date> data, Date value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(convertToDateTime(data));

        return isOKDateTime(dateTimeDB, convertToDateTime(value), (dt) -> {
            return dt.getMonthOfYear();
        });
    }

    public Boolean detectTimeOfDay(List<DateTime> data, DateTime value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(data);

        return isOKDateTime(dateTimeDB, value, (dt) -> {
            return dt.getMinuteOfDay();
        });
    }

    public Boolean detcetTimeOfDay(List<Date> data, Date value) {
        JavaRDD<DateTime> dateTimeDB = sc.parallelize(convertToDateTime(data));

        return isOKDateTime(dateTimeDB, convertToDateTime(value), (dt) -> {
            return dt.getMinuteOfDay();
        });
    }
}

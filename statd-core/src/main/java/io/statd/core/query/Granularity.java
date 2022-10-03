package io.statd.core.query;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public enum Granularity {
    GMin(1, "minute"),
    G5min(2, "5min"),
    GHour(3, "hour"),
    GDay(4, "day"),
    GWeek(5, "week"),
    GMonth(6, "month"),
    GQuarter(7, "quarter"),
    GYear(8, "year"),
    GAll(Integer.MAX_VALUE, "all");//buckets everything into a single bucket

    public final int order;

    private final String name;

    Granularity(int order, String name) {
        this.order = order;
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    public static Granularity from(String value) {

        if (value == null) {
            return GAll;
        }
        switch (value) {
            case "minute":
                return GMin;
            case "5min":
            case "5minute":
                return G5min;
            case "hour":
                return GHour;
            case "day":
                return GDay;
            case "week":
                return GWeek;
            case "month":
                return GMonth;
            case "quarter":
                return GQuarter;
            case "year":
                return GYear;
            case "all":
                return GAll;
            default:
                throw new UnsupportedOperationException("Granularity: '" + value + "' not supported");
        }
    }

    public LocalDateTime getDateTime(LocalDateTime time) {
        switch (this) {
            case GMin:
                return time.truncatedTo(ChronoUnit.MINUTES);
            case G5min:
                return time.withMinute(time.getMinute() - time.getMinute() % 5).truncatedTo(ChronoUnit.MINUTES);
            case GHour:
                return time.truncatedTo(ChronoUnit.HOURS);
            case GDay:
                return time.truncatedTo(ChronoUnit.DAYS);
            case GWeek:
                return time.with(DayOfWeek.MONDAY).truncatedTo(ChronoUnit.DAYS);
            case GMonth:
                return LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0, 0);
            case GQuarter:
                int quarter = (time.getMonth().getValue() - 1) / 3 + 1;
                int firstMonthOfQuarter = (quarter - 1) * 3 + 1;
                return LocalDateTime.of(time.getYear(), firstMonthOfQuarter, 1, 0, 0, 0);
            case GYear:
                return LocalDateTime.of(time.getYear(), 1, 1, 0, 0, 0);
            default:
                return time;
        }
    }


    public LocalDateTime nextPeriod(LocalDateTime time) {
        switch (this) {
            case GMin:
                return time.plusMinutes(1);
            case G5min:
                return time.plusMinutes(5);
            case GHour:
                return time.plusHours(1);
            case GDay:
                return time.plusDays(1);
            case GWeek:
                return time.plusWeeks(1);
            case GMonth:
                return time.plusMonths(1);
            case GQuarter:
                return time.plusMonths(3);
            case GYear:
                return time.plusYears(1);
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    public static boolean isAllGranularity(Granularity granularity) {
        return null == granularity || GAll == granularity;
    }

}
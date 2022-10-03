package io.statd.core.query;

import lombok.Getter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
public final class Interval {

    private LocalDateTime from;
    private LocalDateTime to;

    public Interval(LocalDateTime from, LocalDateTime to) {
        this.from = Objects.requireNonNull(from);
        this.to = Objects.requireNonNull(to);
        this.validate();
    }

    public void validate() {
        Objects.requireNonNull(from, " from date is null");
        Objects.requireNonNull(to, " to date is null");
        if (from.isEqual(to) || from.isAfter(to)) {
            throw new IllegalArgumentException("to date must be after than from date");
        }
    }

    /**
     * 按照粒度获取总的slot
     */
    public int slots(Granularity granularity) {
        LocalDateTime start = from;
        int slots = 0;
        while (start.isBefore(to)) {
            slots += 1;
            start = granularity.nextPeriod(start);
        }
        return slots;
    }


    private List<Interval> intervals(Granularity granularity) {
        Objects.requireNonNull(granularity);
        LocalDateTime start = granularity.getDateTime(from);
        List<Interval> result = new ArrayList<>();
        while (start.isBefore(to)) {
            Interval interval = new Interval(start, granularity.nextPeriod(start));
            result.add(interval);
            start = granularity.nextPeriod(start);
        }
        return result;
    }

    public List<Interval> days() {
        return intervals(Granularity.GDay);
    }


}

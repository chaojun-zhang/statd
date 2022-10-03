package io.statd.core.storage.config.compact;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HourCompact extends CompactConfig {

    @Override
    public TimeType timeType() {
        return TimeType.HOUR_INT;
    }

    @Override
    public int slots() {
        return 12;
    }
}

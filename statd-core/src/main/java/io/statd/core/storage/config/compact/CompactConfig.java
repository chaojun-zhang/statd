package io.statd.core.storage.config.compact;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.statd.core.storage.template.TemplateUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = HourCompact.class, name = "hour"),
        @JsonSubTypes.Type(value = DayCompact.class, name = "day")
})
public abstract class CompactConfig {
    public String timePattern;
    public String slotExpr;
    public int slotStartIdx;
    public String targetFieldName;
    public String metricName;

    public enum TimeType {
        DAY_INT,HOUR_INT
    }

    abstract public TimeType timeType();

    public abstract int slots();

    public List<String> slotColumns(){
        List<String> slotColumns = new ArrayList<>();
        int slots = slots();
        for(int i = 0; i < slots; i++){
            int slot = slotStartIdx + i;
            Map<String,Object> params = new HashMap<>();
            params.put("idx",slot);
            String slotColumn = TemplateUtil.render(slotExpr, params);
            slotColumns.add(slotColumn);
        }
        return slotColumns;
    }
}

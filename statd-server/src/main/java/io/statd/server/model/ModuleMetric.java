package io.statd.server.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class ModuleMetric {
    private String module;
    private String metric;

    public static ModuleMetric of(String module, String metric) {
        ModuleMetric moduleMetric = new ModuleMetric();
        moduleMetric.setModule(module);
        moduleMetric.setMetric(metric);
        return moduleMetric;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModuleMetric that = (ModuleMetric) o;
        return Objects.equals(module, that.module) && Objects.equals(metric, that.metric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(module, metric);
    }

}

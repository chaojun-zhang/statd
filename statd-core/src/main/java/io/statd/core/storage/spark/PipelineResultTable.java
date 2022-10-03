package io.statd.core.storage.spark;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class PipelineResultTable {
    private List<PipelineResultField> schema;
    private List<Map<String, Object>> rows;
}

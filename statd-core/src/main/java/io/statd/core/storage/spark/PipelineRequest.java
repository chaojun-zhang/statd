package io.statd.core.storage.spark;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
public class PipelineRequest {
    private String outputPrefix;
    private List<String> outputs;
    //pipeline sql
    private String pipeline;
    private int limit;

}

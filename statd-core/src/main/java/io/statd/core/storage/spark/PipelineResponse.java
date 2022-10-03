package io.statd.core.storage.spark;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@NoArgsConstructor
public class PipelineResponse {

    private Map<String, PipelineResultTable> tables;
}

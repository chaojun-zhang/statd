package io.statd.core.storage.spark;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Headers;
import retrofit2.http.POST;

public interface SparkJobClient {
    @POST("/v1/pipeline")
    @Headers("Content-Type:application/json")
    Call<PipelineResponse> pipeline(@Body PipelineRequest request);
}

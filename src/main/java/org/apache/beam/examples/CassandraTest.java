/*
package org.apache.beam.examples;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.api.common.ExecutionMode;

import java.util.Collections;

public class CassandraTest {

    public static void main(String args[])
    {



            PipelineOptions options = PipelineOptionsFactory.create();
            options.setRunner(FlinkRunner.class);
            FlinkPipelineOptions flinkPipelineOptions = options.as(FlinkPipelineOptions.class);
            flinkPipelineOptions.setExecutionModeForBatch(ExecutionMode.BATCH.name());
            Pipeline pipeline = Pipeline.create(options);

            PCollection<Scientist> output =
                    pipeline.apply(
                            CassandraIO.<Scientist>read()
                                    .withHosts(Collections.singletonList("172.17.0.2"))
                                    .withPort(9042)
                                    .withKeyspace("apachebeamtest")
                                    .withTable("queryTest")
                                    .withUsername("cassandra")
                                    .withPassword("cassandra")
                                    .withQuery(
                                            "select * from apachebeamtest.queryTest where person_id in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)")
                                    .withCoder(SerializableCoder.of(Scientist.class))
                                    .withEntity(Scientist.class));
                    output.apply(ParDo.of(new CassandraTransform()));

            pipeline.run().waitUntilFinish();


    }

}
*/

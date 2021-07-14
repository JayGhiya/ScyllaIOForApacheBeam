package org.apache.beam.examples;

import org.apache.beam.examples.customSource.ScyllaIO;
import org.apache.beam.examples.data.models.Scientist;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class ScyllaIOTest {

    public static void main(String args[])
    {
        PipelineOptions options = PipelineOptionsFactory.create();
        //FlinkPipelineOptions flinkPipelineOptions = options.as(FlinkPipelineOptions.class);
    //    flinkPipelineOptions.setStreaming(Boolean.FALSE);
       // flinkPipelineOptions.setRunner(FlinkRunner.class);
     //   flinkPipelineOptions.setParallelism(2);
        //Pipeline pipeline = Pipeline.create(flinkPipelineOptions);
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(ScyllaIO.<Scientist>read().withUsername("cassandra")
                .withPassword("cassandra")
                .withKeyspace("apachebeamtest")
                .withTable("queryTest")
                .withQueryWithPartition("select * from apachebeamtest.queryTest where person_id = %d")
                .withPartitions(new ArrayList<>(Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)))
                .withHosts(Collections.singletonList("localhost"))
                .withPort(9042)
                .withMinNumberOfSplits(2)
                .withEntity(Scientist.class)
                .withCoder(SerializableCoder.of(Scientist.class)))
                .apply(ParDo.of(new CassandraTransform()));
        pipeline.run().waitUntilFinish();
    }

}

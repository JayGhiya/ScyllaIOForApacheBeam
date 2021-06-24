/*
package org.apache.beam.examples.customSource;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class ScyllaIO<T> extends BoundedSource<T> {


    static Cluster cluster = Cluster.builder().addContactPoints("172.17.0.2").withPort(19042)
            .withCredentials("cassandra","cassandra")
            .build();

    static Session session = cluster.connect("apachebeamtest");



    @Override
    public List<? extends BoundedSource> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
        return null;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 0;
    }

    @Override
    public BoundedReader createReader(PipelineOptions options) throws IOException {

    }
}
*/

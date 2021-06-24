package org.apache.beam.examples.data.models;



import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Date;

@Table(name = "queryTest", keyspace = "apachebeamtest")
public class Scientist implements Serializable {
    @PartitionKey
    @Column(name = "person_id")
    final int id;

    @Column(name = "person_name")
    final String name;

    @ClusteringColumn
    @Column(name = "person_datetime")
    final Date currentTime;

    Scientist() {
        // Empty constructor needed for deserialization from Cassandra
        this(0, null,null);
    }

    Scientist(int id, String name,Date currentTime) {
        this.id = id;
        this.name = name;
        this.currentTime =currentTime;
    }


}


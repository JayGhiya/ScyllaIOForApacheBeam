package org.apache.beam.examples.data.models;



import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.Data;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Date;


@Table(name = "queryTest", keyspace = "apachebeamtest")
public class Scientist implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    @Column(name = "person_id")
    public int id;

    @Column(name = "person_name")
    public String name;

    @ClusteringColumn
    @Column(name = "person_datetime")
    public Date currentTime;

    public Scientist()
    {

    }
    public Scientist(int id, String name,Date currentTime) {
        this.id = id;
        this.name = name;
        this.currentTime =currentTime;
    }

    @Override
    public boolean equals(Object obj) {
        Scientist other = (Scientist) obj;
        return this.name.equals(other.name) ;
    }


}


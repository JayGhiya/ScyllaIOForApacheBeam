package org.apache.beam.examples;

import org.apache.beam.examples.data.models.Scientist;
import org.apache.beam.sdk.transforms.DoFn;

public class CassandraTransform extends DoFn<Scientist,Scientist> {


    @ProcessElement
    public void processElement(ProcessContext c)
    {
        Scientist scientist = c.element();
        /*System.out.println("scientisit"+scientist.id);
        System.out.println("scientisit"+scientist.name);*/
        c.output(scientist);
    }

}

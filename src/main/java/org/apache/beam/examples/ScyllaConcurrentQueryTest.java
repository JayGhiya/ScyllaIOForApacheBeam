package org.apache.beam.examples;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.*;

public class ScyllaConcurrentQueryTest {


    private  Cluster cluster;

    private Session session;

    public ScyllaConcurrentQueryTest(String contactPoint,int port,String username,String password,String keyspace)
    {
      cluster  = Cluster.builder().addContactPoints(contactPoint).withPort(port)
            .withCredentials(username,password)
            .build();
      session = cluster.connect(keyspace);
    }


    public PreparedStatement initializePreparedStatement(String query)
    {
        PreparedStatement queryLocal = session.prepare(query);
        return queryLocal;
    }







    public ResultSet selectQuery(PreparedStatement preparedStatement, int partitionValue) {
        return session.execute(preparedStatement.bind(partitionValue));

    }

    public void closeResources()
    {
        session.close();
        cluster.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ScyllaConcurrentQueryTest scyllaConcurrentQueryTest = new ScyllaConcurrentQueryTest("172.17.0.2",19042,"cassandra","casandra","apachebeamtest");
        PreparedStatement query = scyllaConcurrentQueryTest.initializePreparedStatement("select * from apachebeamtest.queryTest where person_id = ?");
        ExecutorService executor = Executors.newFixedThreadPool(20);

        List<Future> futures = new ArrayList<>();
        Vector<ResultSet> resultSets = new Vector<>();
        long startTime = System.currentTimeMillis();
        System.out.println("Fetching data from 20 partitions concurrently Started:"+startTime);
        CountDownLatch latch = new CountDownLatch(20);
        for(int i =0;i<20;i++)
        {
            int finalI = i;
            futures.add( executor.submit(() -> {
            resultSets.add(scyllaConcurrentQueryTest.selectQuery(query, finalI));
            latch.countDown();
        }));
        }

        latch.await();

        long endTime = System.currentTimeMillis();

        for(int i =0;i<20;i++) {

           System.out.println("Size of Actual Data:"+resultSets.get(i).all().size());

        }
        System.out.println("size of resultsets:"+resultSets.size());
        System.out.println("Fetching data from 20 partitions concurrently Finished:"+endTime);
        System.out.println("Time spent:"+(endTime-startTime));
        executor.shutdown();
        scyllaConcurrentQueryTest.closeResources();



    }


}

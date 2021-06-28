package org.apache.beam.examples.customSource;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.beam.sdk.options.ValueProvider;

import java.util.List;

/**
 * Spartans class -TODO use same cluster and session throughout parallel requests
 */
public class CommonClusterAndSession<T> {
    private Cluster cluster;
    private Session session;

    private  Cluster getCluster(
            ValueProvider<List<String>> hosts,
            ValueProvider<Integer> port,
            ValueProvider<String> username,
            ValueProvider<String> password,
            ValueProvider<String> localDc,
            ValueProvider<String> consistencyLevel,
            ValueProvider<Integer> connectTimeout,
            ValueProvider<Integer> readTimeout) {

        Cluster.Builder builder =
                Cluster.builder().addContactPoints(hosts.get().toArray(new String[0])).withPort(port.get());

        if (username != null) {
            builder.withAuthProvider(new PlainTextAuthProvider(username.get(), password.get()));
        }

        DCAwareRoundRobinPolicy.Builder dcAwarePolicyBuilder = new DCAwareRoundRobinPolicy.Builder();
        if (localDc != null) {
            dcAwarePolicyBuilder.withLocalDc(localDc.get());
        }

        builder.withLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicyBuilder.build()));

        if (consistencyLevel != null) {
            builder.withQueryOptions(
                    new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.get())));
        }

        SocketOptions socketOptions = new SocketOptions();

        builder.withSocketOptions(socketOptions);

        if (connectTimeout != null) {
            socketOptions.setConnectTimeoutMillis(connectTimeout.get());
        }

        if (readTimeout != null) {
            socketOptions.setReadTimeoutMillis(readTimeout.get());
        }

        return builder.build();
    }

    public CommonClusterAndSession(ScyllaIO.Read<T> spec)
    {
        this.cluster = getCluster(
                             spec.hosts(),
                             spec.port(),
                             spec.username(),
                             spec.password(),
                             spec.localDc(),
                             spec.consistencyLevel(),
                             spec.connectTimeout(),
                             spec.readTimeout());
            //Start of Modification For Scylla
            this.session = this.cluster.connect(spec.keyspace().get());

        }

    public Cluster getCluster()
    {
        return cluster;
    }

    public Session getSession()
    {
        return session;
    }

    public void close()
    {

    }


}

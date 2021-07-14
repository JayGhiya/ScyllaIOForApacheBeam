package org.apache.beam.examples.customSource;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class ScyllaIO {

    private static final Logger LOG = LoggerFactory.getLogger(ScyllaIO.class);


    private ScyllaIO() {
    }

    /**
     * Provide a {@link Read} {@link PTransform} to read data from a Cassandra database.
     */
    public static <T> Read<T> read() {
        return new AutoValue_ScyllaIO_Read.Builder<T>().build();
    }


    /**
     * A {@link PTransform} to read data from Apache Cassandra. See {@link ScyllaIO} for more
     * information on usage and configuration.
     */
    @AutoValue
    @AutoValue.CopyAnnotations
    @SuppressWarnings({"rawtypes"})
    public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

        abstract @Nullable ValueProvider<List<String>> hosts();

        abstract @Nullable ValueProvider<String> queryWithPartitionKey();

        abstract @Nullable ValueProvider<List<Integer>> partitions();

        abstract @Nullable ValueProvider<Integer> port();

        abstract @Nullable ValueProvider<String> keyspace();

        abstract @Nullable ValueProvider<String> table();

        abstract @Nullable Class<T> entity();

        abstract @Nullable Coder<T> coder();

        abstract @Nullable ValueProvider<String> username();

        abstract @Nullable ValueProvider<String> password();

        abstract @Nullable ValueProvider<String> localDc();

        abstract @Nullable ValueProvider<String> consistencyLevel();

        abstract @Nullable ValueProvider<Integer> minNumberOfSplits();

        abstract @Nullable ValueProvider<Integer> connectTimeout();

        abstract @Nullable ValueProvider<Integer> readTimeout();

        abstract @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn();

        abstract Builder<T> builder();

        /**
         * Specify the hosts of the Apache Cassandra instances.
         */
        public Read<T> withPartitions(List<Integer> partitions) {
            checkArgument(partitions != null, "partitions can not be null");
            checkArgument(!partitions.isEmpty(), "partitions can not be empty");
            return withPartitions(ValueProvider.StaticValueProvider.of(partitions));
        }

        /**
         * Specify the hosts of the Apache Cassandra instances.
         */
        public Read<T> withPartitions(ValueProvider<List<Integer>> partitions) {
            return builder().setPartitions(partitions).build();
        }


        /**
         * Specify the hosts of the Apache Cassandra instances.
         */
        public Read<T> withHosts(List<String> hosts) {
            checkArgument(hosts != null, "hosts can not be null");
            checkArgument(!hosts.isEmpty(), "hosts can not be empty");
            return withHosts(ValueProvider.StaticValueProvider.of(hosts));
        }

        /**
         * Specify the hosts of the Apache Cassandra instances.
         */
        public Read<T> withHosts(ValueProvider<List<String>> hosts) {
            return builder().setHosts(hosts).build();
        }

        /**
         * Specify the port number of the Apache Cassandra instances.
         */
        public Read<T> withPort(int port) {
            checkArgument(port > 0, "port must be > 0, but was: %s", port);
            return withPort(ValueProvider.StaticValueProvider.of(port));
        }

        /**
         * Specify the port number of the Apache Cassandra instances.
         */
        public Read<T> withPort(ValueProvider<Integer> port) {
            return builder().setPort(port).build();
        }

        /**
         * Specify the Cassandra keyspace where to read data.
         */
        public Read<T> withKeyspace(String keyspace) {
            checkArgument(keyspace != null, "keyspace can not be null");
            return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
        }

        /**
         * Specify the Cassandra keyspace where to read data.
         */
        public Read<T> withKeyspace(ValueProvider<String> keyspace) {
            return builder().setKeyspace(keyspace).build();
        }

        /**
         * Specify the Cassandra table where to read data.
         */
        public Read<T> withTable(String table) {
            checkArgument(table != null, "table can not be null");
            return withTable(ValueProvider.StaticValueProvider.of(table));
        }

        /**
         * Specify the Cassandra table where to read data.
         */
        public Read<T> withTable(ValueProvider<String> table) {
            return builder().setTable(table).build();
        }

        /**
         * Specify the query to read data.
         */
        public Read<T> withQueryWithPartition(String query) {
            checkArgument(query != null && query.length() > 0, "query cannot be null");
            return withQueryWithPartition(ValueProvider.StaticValueProvider.of(query));
        }

        /**
         * Specify the query to read data.
         */
        public Read<T> withQueryWithPartition(ValueProvider<String> query) {
            return builder().setQueryWithPartitionKey(query).build();
        }

        /**
         * Specify the entity class (annotated POJO). The {@link ScyllaIO} will read the data and
         * convert the data as entity instances. The {@link PCollection} resulting from the read will
         * contains entity elements.
         * @param entity
         */
        public Read<T> withEntity(Class<T> entity) {
            checkArgument(entity != null, "entity can not be null");
            return builder().setEntity(entity).build();
        }

        /**
         * Specify the {@link Coder} used to serialize the entity in the {@link PCollection}.
         */
        public Read<T> withCoder(Coder<T> coder) {
            checkArgument(coder != null, "coder can not be null");
            return builder().setCoder(coder).build();
        }

        /**
         * Specify the username for authentication.
         */
        public Read<T> withUsername(String username) {
            checkArgument(username != null, "username can not be null");
            return withUsername(ValueProvider.StaticValueProvider.of(username));
        }

        /**
         * Specify the username for authentication.
         */
        public Read<T> withUsername(ValueProvider<String> username) {
            return builder().setUsername(username).build();
        }

        /**
         * Specify the password used for authentication.
         */
        public Read<T> withPassword(String password) {
            checkArgument(password != null, "password can not be null");
            return withPassword(ValueProvider.StaticValueProvider.of(password));
        }

        /**
         * Specify the password used for authentication.
         */
        public Read<T> withPassword(ValueProvider<String> password) {
            return builder().setPassword(password).build();
        }

        /**
         * Specify the local DC used for the load balancing.
         */
        public Read<T> withLocalDc(String localDc) {
            checkArgument(localDc != null, "localDc can not be null");
            return withLocalDc(ValueProvider.StaticValueProvider.of(localDc));
        }

        /**
         * Specify the local DC used for the load balancing.
         */
        public Read<T> withLocalDc(ValueProvider<String> localDc) {
            return builder().setLocalDc(localDc).build();
        }

        /**
         * Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc).
         */
        public Read<T> withConsistencyLevel(String consistencyLevel) {
            checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
            return withConsistencyLevel(ValueProvider.StaticValueProvider.of(consistencyLevel));
        }

        /**
         * Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc).
         */
        public Read<T> withConsistencyLevel(ValueProvider<String> consistencyLevel) {
            return builder().setConsistencyLevel(consistencyLevel).build();
        }

        /**
         * It's possible that system.size_estimates isn't populated or that the number of splits
         * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
         * minimum number of splits in case Beam cannot compute it correctly.
         */
        public Read<T> withMinNumberOfSplits(Integer minNumberOfSplits) {
            checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
            checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
            return withMinNumberOfSplits(ValueProvider.StaticValueProvider.of(minNumberOfSplits));
        }

        /**
         * It's possible that system.size_estimates isn't populated or that the number of splits
         * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
         * minimum number of splits in case Beam cannot compute it correctly.
         */
        public Read<T> withMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits) {
            return builder().setMinNumberOfSplits(minNumberOfSplits).build();
        }

        /**
         * Specify the Cassandra client connect timeout in ms. See
         * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-
         */
        public Read<T> withConnectTimeout(Integer timeout) {
            checkArgument(timeout != null, "Connect timeout can not be null");
            checkArgument(timeout > 0, "Connect timeout must be > 0, but was: %s", timeout);
            return withConnectTimeout(ValueProvider.StaticValueProvider.of(timeout));
        }

        /**
         * Specify the Cassandra client connect timeout in ms. See
         * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-
         */
        public Read<T> withConnectTimeout(ValueProvider<Integer> timeout) {
            return builder().setConnectTimeout(timeout).build();
        }

        /**
         * Specify the Cassandra client read timeout in ms. See
         * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-
         */
        public Read<T> withReadTimeout(Integer timeout) {
            checkArgument(timeout != null, "Read timeout can not be null");
            checkArgument(timeout > 0, "Read timeout must be > 0, but was: %s", timeout);
            return withReadTimeout(ValueProvider.StaticValueProvider.of(timeout));
        }

        /**
         * Specify the Cassandra client read timeout in ms. See
         * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-
         */
        public Read<T> withReadTimeout(ValueProvider<Integer> timeout) {
            return builder().setReadTimeout(timeout).build();
        }

        /**
         * A factory to create a specific {@link Mapper} for a given Cassandra Session. This is useful
         * to provide mappers that don't rely in Cassandra annotated objects.
         */
        public Read<T> withMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactory) {
            checkArgument(
                    mapperFactory != null,
                    "CassandraIO.withMapperFactory" + "(withMapperFactory) called with null value");
            return builder().setMapperFactoryFn(mapperFactory).build();
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            checkArgument((hosts() != null && port() != null), "WithHosts() and withPort() are required");
            checkArgument(keyspace() != null, "withKeyspace() is required");
            checkArgument(table() != null, "withTable() is required");
            checkArgument(entity() != null, "withEntity() is required");
            checkArgument(coder() != null, "withCoder() is required");

            return input.apply(org.apache.beam.sdk.io.Read.from(new ScyllaSource<>(this, null)));
        }

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setHosts(ValueProvider<List<String>> hosts);

            abstract Builder<T> setPartitions(ValueProvider<List<Integer>> partitions);

            abstract Builder<T> setQueryWithPartitionKey(ValueProvider<String> queryWithPartition);

            abstract Builder<T> setPort(ValueProvider<Integer> port);

            abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

            abstract Builder<T> setTable(ValueProvider<String> table);

            abstract Builder<T> setEntity(Class<T> entity);

            abstract Optional<Class<T>> entity();

            abstract Builder<T> setCoder(Coder<T> coder);

            abstract Builder<T> setUsername(ValueProvider<String> username);

            abstract Builder<T> setPassword(ValueProvider<String> password);

            abstract Builder<T> setLocalDc(ValueProvider<String> localDc);

            abstract Builder<T> setConsistencyLevel(ValueProvider<String> consistencyLevel);

            abstract Builder<T> setMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits);

            abstract Builder<T> setConnectTimeout(ValueProvider<Integer> timeout);

            abstract Builder<T> setReadTimeout(ValueProvider<Integer> timeout);

            abstract Builder<T> setMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn);

            abstract Optional<SerializableFunction<Session, Mapper>> mapperFactoryFn();

            abstract Read<T> autoBuild();

            public Read<T> build() {
                if (!mapperFactoryFn().isPresent() && entity().isPresent()) {
                    setMapperFactoryFn(new DefaultObjectMapperFactory(entity().get()));
                }
                return autoBuild();
            }
        }
    }

    static class ScyllaSource<T> extends BoundedSource<T> {
        final Read<T> spec;
        final List<String> splitQueries;



        // split source ached size - can't be calculated when already split
        Long estimatedSize;
        private static final String MURMUR3PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

        ScyllaSource(Read<T> spec, List<String> splitQueries) {
            this(spec, splitQueries, null);
        }

        private ScyllaSource(Read<T> spec, List<String> splitQueries, Long estimatedSize) {
            this.estimatedSize = estimatedSize;
            this.spec = spec;
            this.splitQueries = splitQueries;

        }


        @Override
        public Coder<T> getOutputCoder() {
            return spec.coder();
        }

        @Override
        public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
            return new ScyllaReader(this);
        }


        @Override
        public List<BoundedSource<T>> split(
                long desiredBundleSizeBytes, PipelineOptions pipelineOptions) {

            getEstimatedSizeBytes(pipelineOptions);

            return splitAcrossPartitionKeys(spec);


        }

       /* private static String buildQuery(Read spec) {
            return (spec.queryWithPartitionKey() == null)
                    ? String.format("SELECT * FROM %s.%s", spec.keyspace().get(), spec.table().get())
                    : spec.queryWithPartitionKey().get().toString();
        }*/

        /**
         *
         * Spartans Code
         * @param baseQuery
         * @param partitionKey
         * @return
         */
        protected String generatePartitionBasedQuery(String baseQuery, int partitionKey) {
            return String.format(baseQuery,partitionKey);
        }

        /**
         * Spartans Code
         * @param spec
         * @param queries
         * @param estimatedSize
         * @return
         */
        protected BoundedSource<T> generateBoundedSource(Read<T> spec, List<String> queries, long estimatedSize) {
            ScyllaSource<T> source = new ScyllaSource<>(spec, queries, estimatedSize);
            return source;
        }

        /**
         * Spartans Code
         *
         */
        protected List<List<String>> getQueriesListForBoundedSources(int numberOfSplits)
        {
            List<List<String>> queriesListForBoundedSources = new ArrayList<>();

            for(int i =0 ; i <numberOfSplits; i++)
            {
                queriesListForBoundedSources.add(new ArrayList<>());
            }
        return queriesListForBoundedSources;
        }

        protected void distributePartitionBasedQueriesAcross(List<Integer> partitionKeys,List<List<String>> queriesListForBoundedSources,int numberOfSplits)
        {
            int boundedSourceQueriesIndex = 0;
            Iterator<Integer> partitionKeyIterator = partitionKeys.iterator();
            while(partitionKeyIterator.hasNext())
            {
                if(boundedSourceQueriesIndex==numberOfSplits)
                {
                    boundedSourceQueriesIndex=0;
                }

                int partitionKey = partitionKeyIterator.next();

                queriesListForBoundedSources.get(boundedSourceQueriesIndex).add(generatePartitionBasedQuery(spec.queryWithPartitionKey().get(),partitionKey));

                boundedSourceQueriesIndex++;
            }

        }

        protected List<BoundedSource<T>> assignDistributedQueriesListToEachBoundedSource(Read<T> spec,List<List<String>> queriesListForBoundedSources)
        {
            List<BoundedSource<T>> sources = new ArrayList<>();
            for(int i =0 ; i <spec.minNumberOfSplits().get(); i++)
            {
                sources.add(generateBoundedSource(spec,queriesListForBoundedSources.get(i),estimatedSize));
            }
            return sources;
        }


        /**
         * SpartansCode
         * Get data based on single/multiple partition keys
         */
        private List<BoundedSource<T>> splitAcrossPartitionKeys(Read<T> spec) {
            //The max number of splits would be based on number of partition keys
            //After getting the result we will merge n splits into number of task slots available
            List<Integer> partitionKeys = spec.partitions().get();


            int numberOfSplits = spec.minNumberOfSplits().get();

            /**
             * Constructing n number of List which will hold queries for partition keys
             * Here n is equal to number of splits/Bounded Sources
             */
            List<List<String>> queriesListForBoundedSources = getQueriesListForBoundedSources(numberOfSplits);


            /**
             * Distributing partition based queries across queries List
             */

            distributePartitionBasedQueriesAcross(partitionKeys,queriesListForBoundedSources,numberOfSplits);

            /**
             * Assigning queries list to each bounded source
             */

            return assignDistributedQueriesListToEachBoundedSource(spec,queriesListForBoundedSources);

        }

        /**
         * Returns cached estimate for split or if missing calculate size for whole table. Highly
         * innacurate if query is specified.
         *
         * @param pipelineOptions
         * @return
         */
        @Override
        public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {

            if (estimatedSize != null) {
                return estimatedSize;
            } else {
                try (Cluster cluster =
                             getCluster(
                                     spec.hosts(),
                                     spec.port(),
                                     spec.username(),
                                     spec.password(),
                                     spec.localDc(),
                                     spec.consistencyLevel(),
                                     spec.connectTimeout(),
                                     spec.readTimeout())) {
                    if (isMurmur3Partitioner(cluster)) {
                        try {
                            List<TokenRange> tokenRanges =
                                    getTokenRanges(cluster, spec.keyspace().get(), spec.table().get());
                            this.estimatedSize = getEstimatedSizeBytesFromTokenRanges(tokenRanges);
                            return this.estimatedSize;
                        } catch (Exception e) {
                            LOG.warn("Can't estimate the size", e);
                            return 0L;
                        }
                    } else {
                        LOG.warn("Only Murmur3 partitioner is supported, can't estimate the size");
                        return 0L;
                    }
                }
            }
        }

        @VisibleForTesting
        static long getEstimatedSizeBytesFromTokenRanges(List<TokenRange> tokenRanges) {
            long size = 0L;
            for (TokenRange tokenRange : tokenRanges) {
                size = size + tokenRange.meanPartitionSize * tokenRange.partitionCount;
            }
            return Math.round(size / getRingFraction(tokenRanges));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            if (spec.hosts() != null) {
                builder.add(DisplayData.item("hosts", spec.hosts().toString()));
            }
            if (spec.port() != null) {
                builder.add(DisplayData.item("port", spec.port()));
            }
            builder.addIfNotNull(DisplayData.item("keyspace", spec.keyspace()));
            builder.addIfNotNull(DisplayData.item("table", spec.table()));
            builder.addIfNotNull(DisplayData.item("username", spec.username()));
            builder.addIfNotNull(DisplayData.item("localDc", spec.localDc()));
            builder.addIfNotNull(DisplayData.item("consistencyLevel", spec.consistencyLevel()));
        }
        // ------------- CASSANDRA SOURCE UTIL METHODS ---------------//

        /**
         * Gets the list of token ranges that a table occupies on a give Cassandra node.
         *
         * <p>NB: This method is compatible with Cassandra 2.1.5 and greater.
         */
        private  List<TokenRange> getTokenRanges(Cluster cluster,String keyspace, String table) {
            long startTime = System.currentTimeMillis();

            try (Session session = cluster.newSession()) {
                ResultSet resultSet =
                        session.execute(
                                "SELECT range_start, range_end, partitions_count, mean_partition_size FROM "
                                        + "system.size_estimates WHERE keyspace_name = ? AND table_name = ?",
                                keyspace,
                                table);

                ArrayList<TokenRange> tokenRanges = new ArrayList<>();

                for (Row row : resultSet) {
                    TokenRange tokenRange =
                            new TokenRange(
                                    row.getLong("partitions_count"),
                                    row.getLong("mean_partition_size"),
                                    new BigInteger(row.getString("range_start")),
                                    new BigInteger(row.getString("range_end")));
                    tokenRanges.add(tokenRange);
                }
                // The table may not contain the estimates yet
                // or have partitions_count and mean_partition_size fields = 0
                // if the data was just inserted and the amount of data in the table was small.
                // This is very common situation during tests,
                // when we insert a few rows and immediately query them.
                // However, for tiny data sets the lack of size estimates is not a problem at all,
                // because we don't want to split tiny data anyways.
                // Therefore, we're not issuing a warning if the result set was empty
                // or mean_partition_size and partitions_count = 0.
                long endTime =System.currentTimeMillis();
                System.out.println("Time taken for getting token range:"+(endTime-startTime));
                return tokenRanges;
            }
            catch (Exception e)
            {
                LOG.error("Exception encountered while getting tokenRange:",e.getCause());
            }
            return null;
        }

        /**
         * Compute the percentage of token addressed compared with the whole tokens in the cluster.
         */
        @VisibleForTesting
        static double getRingFraction(List<TokenRange> tokenRanges) {
            double ringFraction = 0;
            for (TokenRange tokenRange : tokenRanges) {
                ringFraction =
                        ringFraction
                                + (distance(tokenRange.rangeStart, tokenRange.rangeEnd).doubleValue()
                                / SplitGenerator.getRangeSize(MURMUR3PARTITIONER).doubleValue());
            }
            return ringFraction;
        }

        /**
         * Check if the current partitioner is the Murmur3 (default in Cassandra version newer than 2).
         */
        @VisibleForTesting
        static boolean isMurmur3Partitioner(Cluster cluster) {
            return MURMUR3PARTITIONER.equals(cluster.getMetadata().getPartitioner());
        }

        /**
         * Measure distance between two tokens.
         */
        @VisibleForTesting
        static BigInteger distance(BigInteger left, BigInteger right) {
            return (right.compareTo(left) > 0)
                    ? right.subtract(left)
                    : right.subtract(left).add(SplitGenerator.getRangeSize(MURMUR3PARTITIONER));
        }

        /**
         * Represent a token range in Cassandra instance, wrapping the partition count, size and token
         * range.
         */
        @VisibleForTesting
        static class TokenRange {
            private final long partitionCount;
            private final long meanPartitionSize;
            private final BigInteger rangeStart;
            private final BigInteger rangeEnd;

            TokenRange(
                    long partitionCount, long meanPartitionSize, BigInteger rangeStart, BigInteger rangeEnd) {
                this.partitionCount = partitionCount;
                this.meanPartitionSize = meanPartitionSize;
                this.rangeStart = rangeStart;
                this.rangeEnd = rangeEnd;
            }
        }

        private class ScyllaReader extends BoundedReader<T> {
            private final ScyllaSource<T> source;
            private Cluster cluster;
            private Session session;
            private Iterator<T> iterator;
            private T current;

            ScyllaReader(ScyllaSource<T> source) {
                this.source = source;
            }

            @Override
            public boolean start() {
                LOG.debug("Starting Cassandra reader");
                long startTime = System.currentTimeMillis();
                cluster =
                        getCluster(
                                source.spec.hosts(),
                                source.spec.port(),
                                source.spec.username(),
                                source.spec.password(),
                                source.spec.localDc(),
                                source.spec.consistencyLevel(),
                                source.spec.connectTimeout(),
                                source.spec.readTimeout());
                session = cluster.connect(source.spec.keyspace().get());
                LOG.debug("Queries: " , source.splitQueries);

                List<ResultSetFuture> futures = new ArrayList<>();
                for (String query : source.splitQueries) {
                    futures.add(session.executeAsync(query));
                }

                final Mapper<T> mapper = getMapper(session, source.spec.entity());

                for (ResultSetFuture result : futures) {
                    if (iterator == null) {
                        iterator = mapper.map(result.getUninterruptibly());
                    } else {
                        iterator = Iterators.concat(iterator, mapper.map(result.getUninterruptibly()));
                    }
                }
                long endTime=System.currentTimeMillis();
                LOG.info("Reading Finished for One Bounded Source:",(endTime-startTime));
                return advance();
            }

            @Override
            public boolean advance() {
                if (iterator.hasNext()) {
                    current = iterator.next();
                    return true;
                }
                current = null;
                return false;
            }

            @Override
            public void close() {
                LOG.info("Closing Cassandra reader");
                if (session != null) {
                    session.close();
                }
                if (cluster != null) {
                    cluster.close();
                }
            }

            @Override
            public T getCurrent() throws NoSuchElementException {
                if (current == null) {
                    throw new NoSuchElementException();
                }
                return current;
            }

            @Override
            public ScyllaSource<T> getCurrentSource() {
                return source;
            }

            private Mapper<T> getMapper(Session session, Class<T> enitity) {
                return source.spec.mapperFactoryFn().apply(session);
            }
        }
    }


    /**
     * Get a Cassandra cluster using hosts and port.
     */
    private static Cluster getCluster(
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

    /** Mutator allowing to do side effects into Apache Cassandra database. */

}




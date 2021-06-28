package org.apache.beam.examples.customSource;

import com.datastax.driver.core.Session;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.processing.Generated;
import java.util.List;
import java.util.Optional;

@SuppressWarnings("rawtypes")
@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ScyllaIO_Read<T> extends ScyllaIO.Read<T> {

    private final @Nullable ValueProvider<List<String>> hosts;

    private final @Nullable ValueProvider<String> queryWithPartitionKey;

    private final @Nullable ValueProvider<List<Integer>> partitions;

    private final @Nullable ValueProvider<Integer> port;

    private final @Nullable ValueProvider<String> keyspace;

    private final @Nullable ValueProvider<String> table;

    private final @Nullable Class<T> entity;

    private final @Nullable Coder<T> coder;

    private final @Nullable ValueProvider<String> username;

    private final @Nullable ValueProvider<String> password;

    private final @Nullable ValueProvider<String> localDc;

    private final @Nullable ValueProvider<String> consistencyLevel;

    private final @Nullable ValueProvider<Integer> minNumberOfSplits;

    private final @Nullable ValueProvider<Integer> connectTimeout;

    private final @Nullable ValueProvider<Integer> readTimeout;

    private final @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn;

    private AutoValue_ScyllaIO_Read(
            @Nullable ValueProvider<List<String>> hosts,
            @Nullable ValueProvider<String> queryWithPartitionKey,
            @Nullable ValueProvider<List<Integer>> partitions,
            @Nullable ValueProvider<Integer> port,
            @Nullable ValueProvider<String> keyspace,
            @Nullable ValueProvider<String> table,
            @Nullable Class<T> entity,
            @Nullable Coder<T> coder,
            @Nullable ValueProvider<String> username,
            @Nullable ValueProvider<String> password,
            @Nullable ValueProvider<String> localDc,
            @Nullable ValueProvider<String> consistencyLevel,
            @Nullable ValueProvider<Integer> minNumberOfSplits,
            @Nullable ValueProvider<Integer> connectTimeout,
            @Nullable ValueProvider<Integer> readTimeout,
            @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn) {
        this.hosts = hosts;
        this.queryWithPartitionKey = queryWithPartitionKey;
        this.port = port;
        this.keyspace = keyspace;
        this.table = table;
        this.entity = entity;
        this.coder = coder;
        this.username = username;
        this.password = password;
        this.localDc = localDc;
        this.consistencyLevel = consistencyLevel;
        this.minNumberOfSplits = minNumberOfSplits;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.mapperFactoryFn = mapperFactoryFn;
        this.partitions = partitions;
    }

    @Override
    @Nullable ValueProvider<List<String>> hosts() {
        return hosts;
    }

    @Override
    @Nullable ValueProvider<String> queryWithPartitionKey() {
        return queryWithPartitionKey;
    }

    @Override
    @Nullable ValueProvider<List<Integer>> partitions() {
        return partitions;
    }

    @Override
    @Nullable ValueProvider<Integer> port() {
        return port;
    }

    @Override
    @Nullable ValueProvider<String> keyspace() {
        return keyspace;
    }

    @Override
    @Nullable ValueProvider<String> table() {
        return table;
    }

    @Override
    @Nullable Class<T> entity() {
        return entity;
    }

    @Override
    @Nullable Coder<T> coder() {
        return coder;
    }

    @Override
    @Nullable ValueProvider<String> username() {
        return username;
    }

    @Override
    @Nullable ValueProvider<String> password() {
        return password;
    }

    @Override
    @Nullable ValueProvider<String> localDc() {
        return localDc;
    }

    @Override
    @Nullable ValueProvider<String> consistencyLevel() {
        return consistencyLevel;
    }

    @Override
    @Nullable ValueProvider<Integer> minNumberOfSplits() {
        return minNumberOfSplits;
    }

    @Override
    @Nullable ValueProvider<Integer> connectTimeout() {
        return connectTimeout;
    }

    @Override
    @Nullable ValueProvider<Integer> readTimeout() {
        return readTimeout;
    }

    @Override
    @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn() {
        return mapperFactoryFn;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ScyllaIO.Read) {
            ScyllaIO.Read<?> that = (ScyllaIO.Read<?>) o;
            return (this.hosts == null ? that.hosts() == null : this.hosts.equals(that.hosts()))
                    && (this.partitions == null ? that.partitions() == null : this.partitions.equals(that.partitions()))
                    && (this.queryWithPartitionKey == null ? that.queryWithPartitionKey() == null : this.queryWithPartitionKey.equals(that.queryWithPartitionKey()))
                    && (this.port == null ? that.port() == null : this.port.equals(that.port()))
                    && (this.keyspace == null ? that.keyspace() == null : this.keyspace.equals(that.keyspace()))
                    && (this.table == null ? that.table() == null : this.table.equals(that.table()))
                    && (this.entity == null ? that.entity() == null : this.entity.equals(that.entity()))
                    && (this.coder == null ? that.coder() == null : this.coder.equals(that.coder()))
                    && (this.username == null ? that.username() == null : this.username.equals(that.username()))
                    && (this.password == null ? that.password() == null : this.password.equals(that.password()))
                    && (this.localDc == null ? that.localDc() == null : this.localDc.equals(that.localDc()))
                    && (this.consistencyLevel == null ? that.consistencyLevel() == null : this.consistencyLevel.equals(that.consistencyLevel()))
                    && (this.minNumberOfSplits == null ? that.minNumberOfSplits() == null : this.minNumberOfSplits.equals(that.minNumberOfSplits()))
                    && (this.connectTimeout == null ? that.connectTimeout() == null : this.connectTimeout.equals(that.connectTimeout()))
                    && (this.readTimeout == null ? that.readTimeout() == null : this.readTimeout.equals(that.readTimeout()))
                    && (this.mapperFactoryFn == null ? that.mapperFactoryFn() == null : this.mapperFactoryFn.equals(that.mapperFactoryFn()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h$ = 1;
        h$ *= 1000003;
        h$ ^= (hosts == null) ? 0 : hosts.hashCode();
        h$ *= 1000003;
        h$ ^= (partitions == null) ? 0 : partitions.hashCode();
        h$ *= 1000003;
        h$ ^= (queryWithPartitionKey == null) ? 0 : queryWithPartitionKey.hashCode();
        h$ *= 1000003;
        h$ ^= (port == null) ? 0 : port.hashCode();
        h$ *= 1000003;
        h$ ^= (keyspace == null) ? 0 : keyspace.hashCode();
        h$ *= 1000003;
        h$ ^= (table == null) ? 0 : table.hashCode();
        h$ *= 1000003;
        h$ ^= (entity == null) ? 0 : entity.hashCode();
        h$ *= 1000003;
        h$ ^= (coder == null) ? 0 : coder.hashCode();
        h$ *= 1000003;
        h$ ^= (username == null) ? 0 : username.hashCode();
        h$ *= 1000003;
        h$ ^= (password == null) ? 0 : password.hashCode();
        h$ *= 1000003;
        h$ ^= (localDc == null) ? 0 : localDc.hashCode();
        h$ *= 1000003;
        h$ ^= (consistencyLevel == null) ? 0 : consistencyLevel.hashCode();
        h$ *= 1000003;
        h$ ^= (minNumberOfSplits == null) ? 0 : minNumberOfSplits.hashCode();
        h$ *= 1000003;
        h$ ^= (connectTimeout == null) ? 0 : connectTimeout.hashCode();
        h$ *= 1000003;
        h$ ^= (readTimeout == null) ? 0 : readTimeout.hashCode();
        h$ *= 1000003;
        h$ ^= (mapperFactoryFn == null) ? 0 : mapperFactoryFn.hashCode();
        return h$;
    }

    @Override
    ScyllaIO.Read.Builder<T> builder() {
        return new Builder<T>(this);
    }

    static final class Builder<T> extends ScyllaIO.Read.Builder<T> {
        private @Nullable ValueProvider<List<String>> hosts;
        private @Nullable ValueProvider<String> queryWithPartitionKey;
        private @Nullable ValueProvider<List<Integer>> partitions;
        private @Nullable ValueProvider<Integer> port;
        private @Nullable ValueProvider<String> keyspace;
        private @Nullable ValueProvider<String> table;
        private @Nullable Class<T> entity;
        private @Nullable Coder<T> coder;
        private @Nullable ValueProvider<String> username;
        private @Nullable ValueProvider<String> password;
        private @Nullable ValueProvider<String> localDc;
        private @Nullable ValueProvider<String> consistencyLevel;
        private @Nullable ValueProvider<Integer> minNumberOfSplits;
        private @Nullable ValueProvider<Integer> connectTimeout;
        private @Nullable ValueProvider<Integer> readTimeout;
        private @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn;
        Builder() {
        }
        private Builder(ScyllaIO.Read<T> source) {
            this.hosts = source.hosts();
            this.queryWithPartitionKey = source.queryWithPartitionKey();
            this.port = source.port();
            this.keyspace = source.keyspace();
            this.table = source.table();
            this.entity = source.entity();
            this.coder = source.coder();
            this.username = source.username();
            this.password = source.password();
            this.localDc = source.localDc();
            this.consistencyLevel = source.consistencyLevel();
            this.minNumberOfSplits = source.minNumberOfSplits();
            this.connectTimeout = source.connectTimeout();
            this.readTimeout = source.readTimeout();
            this.mapperFactoryFn = source.mapperFactoryFn();
            this.partitions = source.partitions();
        }
        @Override
        ScyllaIO.Read.Builder<T> setHosts(ValueProvider<List<String>> hosts) {
            this.hosts = hosts;
            return this;
        }

        @Override
        ScyllaIO.Read.Builder<T> setPartitions(ValueProvider<List<Integer>> partitions) {
            this.partitions =partitions;
            return this;
        }



        @Override
        ScyllaIO.Read.Builder<T> setQueryWithPartitionKey(ValueProvider<String> query) {
            this.queryWithPartitionKey = query;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setPort(ValueProvider<Integer> port) {
            this.port = port;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setKeyspace(ValueProvider<String> keyspace) {
            this.keyspace = keyspace;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setTable(ValueProvider<String> table) {
            this.table = table;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setEntity(Class<T> entity) {
            this.entity = entity;
            return this;
        }
        @Override
        Optional<Class<T>> entity() {
            if (entity == null) {
                return Optional.empty();
            } else {
                return Optional.of(entity);
            }
        }
        @Override
        ScyllaIO.Read.Builder<T> setCoder(Coder<T> coder) {
            this.coder = coder;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setUsername(ValueProvider<String> username) {
            this.username = username;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setPassword(ValueProvider<String> password) {
            this.password = password;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setLocalDc(ValueProvider<String> localDc) {
            this.localDc = localDc;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setConsistencyLevel(ValueProvider<String> consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits) {
            this.minNumberOfSplits = minNumberOfSplits;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setConnectTimeout(ValueProvider<Integer> connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }
        @Override
        ScyllaIO.Read.Builder<T> setReadTimeout(ValueProvider<Integer> readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        @Override
        ScyllaIO.Read.Builder<T> setMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn) {
            this.mapperFactoryFn = mapperFactoryFn;
            return this;
        }

        @Override
        Optional<SerializableFunction<Session, Mapper>> mapperFactoryFn() {
            if (mapperFactoryFn == null) {
                return Optional.empty();
            } else {
                return Optional.of(mapperFactoryFn);
            }
        }
        @Override
        ScyllaIO.Read<T> autoBuild() {
            return new AutoValue_ScyllaIO_Read<T>(
                    this.hosts,
                    this.queryWithPartitionKey,
                    this.partitions,
                    this.port,
                    this.keyspace,
                    this.table,
                    this.entity,
                    this.coder,
                    this.username,
                    this.password,
                    this.localDc,
                    this.consistencyLevel,
                    this.minNumberOfSplits,
                    this.connectTimeout,
                    this.readTimeout,
                    this.mapperFactoryFn);
        }
    }


}

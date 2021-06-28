package org.apache.beam.examples.customSource;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.apache.beam.sdk.transforms.SerializableFunction;

class DefaultObjectMapperFactory<T> implements SerializableFunction<Session, Mapper> {

  private transient MappingManager mappingManager;
  Class<T> entity;

  DefaultObjectMapperFactory(Class<T> entity) {
    this.entity = entity;
  }

  @Override
  public Mapper apply(Session session) {
    if (mappingManager == null) {
      this.mappingManager = new MappingManager(session);
    }

    return new DefaultObjectMapper<T>(mappingManager.mapper(entity));
  }
}

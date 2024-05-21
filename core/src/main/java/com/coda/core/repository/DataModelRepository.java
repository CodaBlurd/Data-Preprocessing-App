package com.coda.core.repository;

import com.coda.core.entities.DataModel;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * DataModelRepository is a repository interface that extends MongoRepository.
 * <p>
 * It represents the repository for the DataModel entity.
 */
@Repository
public interface DataModelRepository
        extends MongoRepository<DataModel<?>, String> {

}

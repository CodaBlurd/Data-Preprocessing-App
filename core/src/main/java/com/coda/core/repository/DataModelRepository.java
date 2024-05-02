package com.coda.core.repository;
import com.coda.core.entities.DataModel;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * Repository class for DataModel entity
 * <p> This class is responsible for all the database operations related to DataModel entity</p>
 * {@code @Repository} annotation to mark the class as a repository class
 * The data model class will hold the data set model to be cleaned
 *
 */

@Repository
public interface DataModelRepository extends MongoRepository<DataModel, String> {

}

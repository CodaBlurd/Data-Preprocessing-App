package com.coda.core.service;

import com.coda.core.entities.DataModel;
import com.coda.core.repository.DataModelRepository;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.FluentQuery;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class RepoTestImpl implements DataModelRepository {
    @Override
    public <S extends DataModel<?>> S insert(S entity) {
        return null;
    }

    @Override
    public <S extends DataModel<?>> List<S> insert(Iterable<S> entities) {
        return null;
    }

    @Override
    public <S extends DataModel<?>> Optional<S> findOne(Example<S> example) {
        return Optional.empty();
    }

    @Override
    public <S extends DataModel<?>> List<S> findAll(Example<S> example) {
        return null;
    }

    @Override
    public <S extends DataModel<?>> List<S> findAll(Example<S> example, Sort sort) {
        return null;
    }

    @Override
    public <S extends DataModel<?>> Page<S> findAll(Example<S> example, Pageable pageable) {
        return null;
    }

    @Override
    public <S extends DataModel<?>> long count(Example<S> example) {
        return 0;
    }

    @Override
    public <S extends DataModel<?>> boolean exists(Example<S> example) {
        return false;
    }

    @Override
    public <S extends DataModel<?>, R> R findBy(Example<S> example, Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) {
        return null;
    }

    @Override
    public <S extends DataModel<?>> S save(S entity) {
        return null;
    }

    @Override
    public <S extends DataModel<?>> List<S> saveAll(Iterable<S> entities) {
        return null;
    }

    @Override
    public Optional<DataModel<?>> findById(String s) {
        return Optional.empty();
    }

    @Override
    public boolean existsById(String s) {
        return false;
    }

    @Override
    public List<DataModel<?>> findAll() {
        return null;
    }

    @Override
    public List<DataModel<?>> findAllById(Iterable<String> strings) {
        return null;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public void deleteById(String s) {

    }

    @Override
    public void delete(DataModel<?> entity) {

    }

    @Override
    public void deleteAllById(Iterable<? extends String> strings) {

    }

    @Override
    public void deleteAll(Iterable<? extends DataModel<?>> entities) {

    }

    @Override
    public void deleteAll() {

    }

    @Override
    public List<DataModel<?>> findAll(Sort sort) {
        return null;
    }

    @Override
    public Page<DataModel<?>> findAll(Pageable pageable) {
        return null;
    }
}

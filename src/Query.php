<?php

namespace As3\Modlr\Persister\MongoDb;

use As3\Modlr\Metadata\EntityMetadata;
use As3\Modlr\Persister\PersisterException;
use As3\Modlr\Store\Store;
use Doctrine\MongoDB\Connection;
use Doctrine\MongoDB\Query\Builder as QueryBuilder;

/**
 * Handles query operations for a MongoDB database connection.
 *
 * @author Jacob Bare <jacob.bare@gmail.com>
 */
final class Query
{
    /**
     * The Doctine MongoDB connection.
     *
     * @var Connection
     */
    private $connection;

    /**
     * The query/database operations formatter.
     *
     * @var Formatter
     */
    private $formatter;

    /**
     * Constructor.
     *
     * @param   Connection  $connection
     * @param   Formatter   $formatter
     */
    public function __construct(Connection $connection, Formatter $formatter)
    {
        $this->connection = $connection;
        $this->formatter = $formatter;
    }

    /**
     * Creates a builder object for querying MongoDB based on the provided metadata.
     *
     * @param   EntityMetadata  $metadata
     * @return  QueryBuilder
     */
    public function createQueryBuilder(EntityMetadata $metadata)
    {
        return $this->getModelCollection($metadata)->createQueryBuilder();
    }

    /**
     * Executes a delete for the provided metadata and criteria.
     *
     * @param   EntityMetadata  $metadata
     * @param   Store           $store
     * @param   array           $criteria
     * @return  array|bool
     */
    public function executeDelete(EntityMetadata $metadata, Store $store, array $criteria)
    {
        $criteria = $this->getFormatter()->formatQuery($metadata, $store, $criteria);
        return $this->createQueryBuilder($metadata)
            ->remove()
            ->setQueryArray($criteria)
            ->getQuery()
            ->execute();
        ;
    }

    /**
     * Finds records from the database based on the provided metadata and criteria.
     *
     * @param   EntityMetadata  $metadata   The model metadata that the database should query against.
     * @param   Store           $store      The store.
     * @param   array           $criteria   The query criteria.
     * @param   array           $fields     Fields to include/exclude.
     * @param   array           $sort       The sort criteria.
     * @param   int             $offset     The starting offset, aka the number of Models to skip.
     * @param   int             $limit      The number of Models to limit.
     * @return  \Doctrine\MongoDB\Cursor
     */
    public function executeFind(EntityMetadata $metadata, Store $store, array $criteria, array $fields = [], array $sort = [], $offset = 0, $limit = 0)
    {
        $criteria = $this->getFormatter()->formatQuery($metadata, $store, $criteria);

        $builder = $this->createQueryBuilder($metadata)
            ->find()
            ->setQueryArray($criteria)
        ;

        $this->appendSearch($builder, $criteria);
        $this->appendFields($builder, $fields);
        $this->appendSort($builder, $sort);
        $this->appendLimitAndOffset($builder, $limit, $offset);

        return $builder->getQuery()->execute();
    }

    /**
     * Executes an insert for the provided metadata.
     *
     * @param   EntityMetadata  $metadata
     * @param   array           $toInsert
     * @return  array|bool
     */
    public function executeInsert(EntityMetadata $metadata, array $toInsert)
    {
        return $this->createQueryBuilder($metadata)
            ->insert()
            ->setNewObj($toInsert)
            ->getQuery()
            ->execute()
        ;
    }

    /**
     * Updates a record from the database based on the provided metadata and criteria.
     *
     * @param   EntityMetadata  $metadata   The model metadata that the database should query against.
     * @param   Store           $store      The store.
     * @param   array           $criteria   The query criteria.
     * @param   array           $toUpdate   The data to update.
     * @return  array|bool
     */
    public function executeUpdate(EntityMetadata $metadata, Store $store, array $criteria, array $toUpdate)
    {
        $criteria = $this->getFormatter()->formatQuery($metadata, $store, $criteria);
        return $this->createQueryBuilder($metadata)
            ->update()
            ->setQueryArray($criteria)
            ->setNewObj($toUpdate)
            ->getQuery()
            ->execute();
        ;
    }

    /**
     * @return  Formatter
     */
    public function getFormatter()
    {
        return $this->formatter;
    }

    /**
     * Gets standard database retrieval criteria for an inverse relationship.
     *
     * @param   EntityMetadata  $owner
     * @param   EntityMetadata  $related
     * @param   string|array    $identifiers
     * @param   string          $inverseField
     * @return  array
     */
    public function getInverseCriteria(EntityMetadata $owner, EntityMetadata $related, $identifiers, $inverseField)
    {
        $criteria = [
            $inverseField   => (array) $identifiers,
        ];
        if (true === $related->isChildEntity()) {
            // The relationship is owned by a polymorphic model. Must include the type in the root criteria.
            $criteria[Persister::POLYMORPHIC_KEY] = $related->type;
        }
        return $criteria;
    }

    /**
     * Gets the MongoDB Collection object for a Model.
     *
     * @param   EntityMetadata  $metadata
     * @return  \Doctrine\MongoDB\Collection
     */
    public function getModelCollection(EntityMetadata $metadata)
    {
        if (!$metadata->persistence instanceof StorageMetadata) {
            throw PersisterException::badRequest('Wrong StorageMetadata type');
        }
        return $this->connection->selectCollection($metadata->persistence->db, $metadata->persistence->collection);
    }

    /**
     * Gets standard database retrieval criteria for an entity and the provided identifiers.
     *
     * @param   EntityMetadata      $metadata       The entity to retrieve database records for.
     * @param   string|array|null   $identifiers    The IDs to query.
     * @return  array
     */
    public function getRetrieveCritiera(EntityMetadata $metadata, $identifiers = null)
    {
        $criteria = [];
        if (true === $metadata->isChildEntity()) {
            $criteria[Persister::POLYMORPHIC_KEY] = $metadata->type;
        }

        $identifiers = (array) $identifiers;
        if (empty($identifiers)) {
            return $criteria;
        }
        $criteria[Persister::IDENTIFIER_KEY] = (1 === count($identifiers)) ? reset($identifiers) : $identifiers;
        return $criteria;
    }

    /**
     * Appends projection fields to a Query Builder.
     *
     * @param   QueryBuilder    $builder
     * @param   array           $fields
     * @return  self
     */
    private function appendFields(QueryBuilder $builder, array $fields)
    {
        list($fields, $include) = $this->prepareFields($fields);
        if (!empty($fields)) {
            $method = (true === $include) ? 'select' : 'exclude';
            $builder->$method(array_keys($fields));
        }
        return $this;
    }

    /**
     * Appends offset and limit criteria to a Query Builder
     *
     * @param   QueryBuilder    $builder
     * @param   int             $limit
     * @param   int             $offset
     * @return  self
     */
    private function appendLimitAndOffset(QueryBuilder $builder, $limit, $offset)
    {
        $limit = (int) $limit;
        $offset = (int) $offset;

        if ($limit > 0) {
            $builder->limit($limit);
        }

        if ($offset > 0) {
            $builder->skip($offset);
        }
        return $this;
    }

    /**
     * Appends text search score and sorting to a Query Builder.
     *
     * @param   QueryBuilder    $builder
     * @param   array           $criteria
     * @return  self
     */
    private function appendSearch(QueryBuilder $builder, array $criteria)
    {
        if (false === $this->isSearchQuery($criteria)) {
            return $this;
        }
        $builder->selectMeta('searchScore', 'textScore');
        $builder->sortMeta('searchScore', 'textScore');
        return $this;
    }

    /**
     * Appends sorting criteria to a Query Builder.
     *
     * @param   QueryBuilder    $builder
     * @param   array           $sort
     * @return  self
     */
    private function appendSort(QueryBuilder $builder, array $sort)
    {
        if (!empty($sort)) {
            $builder->sort($sort);
        }
        return $this;
    }

    /**
     * Determines if the provided query criteria contains text search.
     *
     * @param   array   $criteria
     * @return  bool
     */
    private function isSearchQuery(array $criteria)
    {
        if (isset($criteria['$text'])) {
            return true;
        }
        foreach ($criteria as $key => $value) {
            if (is_array($value) && true === $this->isSearchQuery($value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Prepares projection fields for a query and returns as a tuple.
     *
     * @param   array   $fields
     * @return  array
     * @throws  PersisterException
     */
    private function prepareFields(array $fields)
    {
        $include = null;
        foreach ($fields as $key => $type) {
            $type = (bool) $type;
            if (null === $include) {
                $include = $type;
            }
            if ($type !== $include) {
                PersisterException::badRequest('Field projection mismatch. You cannot both exclude and include fields.');
            }
            $fields[$key] = $type;
        }
        return [$fields, $include];
    }
}

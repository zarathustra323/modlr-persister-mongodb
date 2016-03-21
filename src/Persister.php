<?php

namespace As3\Modlr\Persister\MongoDb;

use As3\Modlr\Store\Store;
use As3\Modlr\Models\Model;
use As3\Modlr\Models\Collection;
use As3\Modlr\Metadata\EntityMetadata;
use As3\Modlr\Metadata\AttributeMetadata;
use As3\Modlr\Metadata\RelationshipMetadata;
use As3\Modlr\Persister\PersisterInterface;
use As3\Modlr\Persister\PersisterException;
use As3\Modlr\Persister\Record;
use Doctrine\MongoDB\Connection;
use \MongoId;

/**
 * Persists and retrieves models to/from a MongoDB database.
 *
 * @author Jacob Bare <jacob.bare@gmail.com>
 */
final class Persister implements PersisterInterface
{
    const IDENTIFIER_KEY    = '_id';
    const POLYMORPHIC_KEY   = '_type';
    const PERSISTER_KEY     = 'mongodb';

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
     * The raw result hydrator.
     *
     * @var Hydrator
     */
    private $hydrator;

    /**
     * @var StorageMetadataFactory
     */
    private $smf;

    /**
     * Constructor.
     *
     * @param   Connection              $connection
     * @param   StorageMetadataFactory  $smf
     */
    public function __construct(Connection $connection, StorageMetadataFactory $smf)
    {
        $this->connection = $connection;
        $this->formatter = new Formatter();
        $this->hydrator = new Hydrator();
        $this->smf = $smf;

    }

    /**
     * {@inheritDoc}
     */
    public function getPersisterKey()
    {
        return self::PERSISTER_KEY;
    }

    /**
     * {@inheritDoc}
     */
    public function getPersistenceMetadataFactory()
    {
        return $this->smf;
    }

    /**
     * {@inheritDoc}
     * @todo    Implement sorting and pagination (limit/skip).
     */
    public function all(EntityMetadata $metadata, Store $store, array $identifiers = [])
    {
        $criteria = $this->getRetrieveCritiera($metadata, $identifiers);
        $cursor = $this->doQuery($metadata, $store, $criteria);
        return $this->getHydrator()->hydrateMany($metadata, $cursor->toArray(), $store);
    }

    /**
     * {@inheritDoc}
     */
    public function query(EntityMetadata $metadata, Store $store, array $criteria, array $fields = [], array $sort = [], $offset = 0, $limit = 0)
    {
        $cursor = $this->doQuery($metadata, $store, $criteria);
        return $this->getHydrator()->hydrateMany($metadata, $cursor->toArray(), $store);
    }

    /**
     * {@inheritDoc}
     */
    public function inverse(EntityMetadata $owner, EntityMetadata $rel, Store $store, array $identifiers, $inverseField)
    {
        $criteria = $this->getInverseCriteria($owner, $rel, $identifiers, $inverseField);
        $cursor = $this->doQuery($rel, $store, $criteria);
        return $this->getHydrator()->hydrateMany($rel, $cursor->toArray(), $store);
    }

    /**
     * {@inheritDoc}
     */
    public function retrieve(EntityMetadata $metadata, $identifier, Store $store)
    {
        $criteria = $this->getRetrieveCritiera($metadata, $identifier);
        $result = $this->doQuery($metadata, $store, $criteria)->getSingleResult();
        if (null === $result) {
            return;
        }
        return $this->getHydrator()->hydrateOne($metadata, $result, $store);
    }

    /**
     * {@inheritDoc}
     * @todo    Optimize the changeset to query generation.
     */
    public function create(Model $model)
    {
        $metadata = $model->getMetadata();
        $insert[$this->getIdentifierKey()] = $this->convertId($model->getId());
        if (true === $metadata->isChildEntity()) {
            $insert[$this->getPolymorphicKey()] = $metadata->type;
        }

        $changeset = $model->getChangeSet();
        foreach ($changeset['attributes'] as $key => $values) {
            $value = $this->getFormatter()->getAttributeDbValue($metadata->getAttribute($key), $values['new']);
            if (null === $value) {
                continue;
            }
            $insert[$key] = $value;
        }
        foreach ($changeset['hasOne'] as $key => $values) {
            $value = $this->getFormatter()->getHasOneDbValue($metadata->getRelationship($key), $values['new']);
            if (null === $value) {
                continue;
            }
            $insert[$key] = $value;
        }
        foreach ($changeset['hasMany'] as $key => $values) {
            $value = $this->getFormatter()->getHasManyDbValue($metadata->getRelationship($key), $values['new']);
            if (null === $value) {
                continue;
            }
            $insert[$key] = $value;
        }
        $this->createQueryBuilder($metadata)
            ->insert()
            ->setNewObj($insert)
            ->getQuery()
            ->execute()
        ;
        return $model;
    }

    /**
     * {@inheritDoc}
     * @todo    Optimize the changeset to query generation.
     */
    public function update(Model $model)
    {
        $metadata = $model->getMetadata();
        $criteria = $this->getRetrieveCritiera($metadata, $model->getId());
        $changeset = $model->getChangeSet();

        $update = [];
        foreach ($changeset['attributes'] as $key => $values) {
            if (null === $values['new']) {
                $op = '$unset';
                $value = 1;
            } else {
                $op = '$set';
                $value = $this->getFormatter()->getAttributeDbValue($metadata->getAttribute($key), $values['new']);
            }
            $update[$op][$key] = $value;
        }

        // @todo Must prevent inverse relationships from persisting
        foreach ($changeset['hasOne'] as $key => $values) {
            if (null === $values['new']) {
                $op = '$unset';
                $value = 1;
            } else {
                $op = '$set';
                $value = $this->getFormatter()->getHasOneDbValue($metadata->getRelationship($key), $values['new']);
            }
            $update[$op][$key] = $value;
        }

        foreach ($changeset['hasMany'] as $key => $values) {
            if (null === $values['new']) {
                $op = '$unset';
                $value = 1;
            } else {
                $op = '$set';
                $value = $this->getFormatter()->getHasManyDbValue($metadata->getRelationship($key), $values['new']);
            }
            $update[$op][$key] = $value;
        }

        if (empty($update)) {
            return $model;
        }

        $this->createQueryBuilder($metadata)
            ->update()
            ->setQueryArray($criteria)
            ->setNewObj($update)
            ->getQuery()
            ->execute();
        ;
        return $model;
    }

    /**
     * {@inheritDoc}
     */
    public function delete(Model $model)
    {
        $metadata = $model->getMetadata();
        $criteria = $this->getRetrieveCritiera($metadata, $model->getId());

        $this->createQueryBuilder($metadata)
            ->remove()
            ->setQueryArray($criteria)
            ->getQuery()
            ->execute();
        ;
        return $model;
    }

    /**
     * {@inheritDoc}
     */
    public function generateId($strategy = null)
    {
        if (false === $this->getFormatter()->isIdStrategySupported($strategy)) {
            throw PersisterException::nyi('ID generation currently only supports an object strategy, or none at all.');
        }
        return new MongoId();
    }

    /**
     * @return  Formatter
     */
    public function getFormatter()
    {
        return $this->formatter;
    }

    /**
     * @return  Hydrator
     */
    public function getHydrator()
    {
        return $this->hydrator;
    }

    /**
     * {@inheritDoc}
     */
    public function convertId($identifier, $strategy = null)
    {
        return $this->getFormatter()->getIdentifierDbValue($identifier, $strategy);
    }

    /**
     * {@inheritDoc}
     */
    public function getIdentifierKey()
    {
        return self::IDENTIFIER_KEY;
    }

    /**
     * {@inheritDoc}
     */
    public function getPolymorphicKey()
    {
        return self::POLYMORPHIC_KEY;
    }

    /**
     * {@inheritDoc}
     */
    public function extractType(EntityMetadata $metadata, array $data)
    {
        return $this->getHydrator()->extractType($metadata, $data);
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
    protected function doQuery(EntityMetadata $metadata, Store $store, array $criteria, array $fields = [], array $sort = [], $offset = 0, $limit = 0)
    {
        $criteria = $this->getFormatter()->formatQuery($metadata, $store, $criteria);
        return $this->createQueryBuilder($metadata)
            ->find()
            ->setQueryArray($criteria)
            ->getQuery()
            ->execute()
        ;
    }

    /**
     * Gets standard database retrieval criteria for an inverse relationship.
     *
     * @param   EntityMetadata  $metadata       The entity to retrieve database records for.
     * @param   string|array    $identifiers    The IDs to query.
     * @return  array
     */
    protected function getInverseCriteria(EntityMetadata $owner, EntityMetadata $related, $identifiers, $inverseField)
    {
        $criteria[$inverseField] = (array) $identifiers;
        if (true === $owner->isChildEntity()) {
            // The owner is owned by a polymorphic model. Must include the type with the inverse field criteria.
            $criteria[$inverseField] = [
                $this->getIdentifierKey()   => $criteria[$inverseField],
                $this->getPolymorphicKey()  => $owner->type,
            ];
        }
        if (true === $related->isChildEntity()) {
            // The relationship is owned by a polymorphic model. Must include the type in the root criteria.
            $criteria[$this->getPolymorphicKey()] = $related->type;
        }
        return $criteria;
    }

    /**
     * Gets standard database retrieval criteria for an entity and the provided identifiers.
     *
     * @param   EntityMetadata      $metadata       The entity to retrieve database records for.
     * @param   string|array|null   $identifiers    The IDs to query.
     * @return  array
     */
    protected function getRetrieveCritiera(EntityMetadata $metadata, $identifiers = null)
    {
        $criteria = [];
        if (true === $metadata->isChildEntity()) {
            $criteria[$this->getPolymorphicKey()] = $metadata->type;
        }

        if (null === $identifiers) {
            return $criteria;
        }
        $identifiers = (array) $identifiers;
        if (empty($identifiers)) {
            return $criteria;
        }
        $criteria[$this->getIdentifierKey()] = (1 === count($identifiers)) ? $identifiers[0] : $identifiers;
        return $criteria;
    }

    /**
     * Creates a builder object for querying MongoDB based on the provided metadata.
     *
     * @param   EntityMetadata  $metadata
     * @return  \Doctrine\MongoDB\Query\Builder
     */
    protected function createQueryBuilder(EntityMetadata $metadata)
    {
        return $this->getModelCollection($metadata)->createQueryBuilder();
    }

    /**
     * Gets the MongoDB Collection object for a Model.
     *
     * @param   EntityMetadata  $metadata
     * @return  \Doctrine\MongoDB\Collection
     */
    protected function getModelCollection(EntityMetadata $metadata)
    {
        return $this->connection->selectCollection($metadata->persistence->db, $metadata->persistence->collection);
    }
}

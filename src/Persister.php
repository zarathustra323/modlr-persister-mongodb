<?php

namespace As3\Modlr\Persister\MongoDb;

use \MongoId;
use As3\Modlr\Metadata\EntityMetadata;
use As3\Modlr\Models\Model;
use As3\Modlr\Persister\PersisterException;
use As3\Modlr\Persister\PersisterInterface;
use As3\Modlr\Store\Store;
use Doctrine\MongoDB\Connection;

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
     * The query service.
     *
     * @var Query
     */
    private $query;

    /**
     * Constructor.
     *
     * @param   Query                   $query
     * @param   StorageMetadataFactory  $smf
     */
    public function __construct(Query $query, StorageMetadataFactory $smf)
    {
        $this->hydrator = new Hydrator();
        $this->smf = $smf;
        $this->query = $query;
    }

    /**
     * {@inheritDoc}
     */
    public function all(EntityMetadata $metadata, Store $store, array $identifiers = [])
    {
        $criteria = $this->getQuery()->getRetrieveCritiera($metadata, $identifiers);
        $cursor = $this->getQuery()->executeFind($metadata, $store, $criteria);
        return $this->getHydrator()->hydrateMany($metadata, $cursor->toArray(), $store);
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

        $this->getQuery()->executeInsert($metadata, $insert);
        return $model;
    }

    /**
     * {@inheritDoc}
     */
    public function delete(Model $model)
    {
        $metadata = $model->getMetadata();
        $criteria = $this->getQuery()->getRetrieveCritiera($metadata, $model->getId());
        $this->getQuery()->executeDelete($metadata, $model->getStore(), $criteria);
        return $model;
    }

    /**
     * {@inheritDoc}
     */
    public function extractType(EntityMetadata $metadata, array $data)
    {
        return $this->getHydrator()->extractType($metadata, $data);
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
        return $this->getQuery()->getFormatter();
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
    public function getIdentifierKey()
    {
        return self::IDENTIFIER_KEY;
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
     */
    public function getPersisterKey()
    {
        return self::PERSISTER_KEY;
    }

    /**
     * {@inheritDoc}
     */
    public function getPolymorphicKey()
    {
        return self::POLYMORPHIC_KEY;
    }

    /**
     * @return Query
     */
    public function getQuery()
    {
        return $this->query;
    }

    /**
     * {@inheritDoc}
     */
    public function inverse(EntityMetadata $owner, EntityMetadata $rel, Store $store, array $identifiers, $inverseField)
    {
        $criteria = $this->getQuery()->getInverseCriteria($owner, $rel, $identifiers, $inverseField);
        $cursor = $this->getQuery()->executeFind($rel, $store, $criteria);
        return $this->getHydrator()->hydrateMany($rel, $cursor->toArray(), $store);
    }

    /**
     * {@inheritDoc}
     */
    public function query(EntityMetadata $metadata, Store $store, array $criteria, array $fields = [], array $sort = [], $offset = 0, $limit = 0)
    {
        $cursor = $this->getQuery()->executeFind($metadata, $store, $criteria);
        return $this->getHydrator()->hydrateMany($metadata, $cursor->toArray(), $store);
    }

    /**
     * {@inheritDoc}
     */
    public function retrieve(EntityMetadata $metadata, $identifier, Store $store)
    {
        $criteria = $this->getQuery()->getRetrieveCritiera($metadata, $identifier);
        $result = $this->getQuery()->executeFind($metadata, $store, $criteria)->getSingleResult();
        if (null === $result) {
            return;
        }
        return $this->getHydrator()->hydrateOne($metadata, $result, $store);
    }

    /**
     * {@inheritDoc}
     * @todo    Optimize the changeset to query generation.
     */
    public function update(Model $model)
    {
        $metadata = $model->getMetadata();
        $criteria = $this->getQuery()->getRetrieveCritiera($metadata, $model->getId());
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

        $this->getQuery()->executeUpdate($metadata, $model->getStore(), $criteria, $update);
        return $model;
    }
}

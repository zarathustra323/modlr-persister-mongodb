<?php

namespace As3\Modlr\Persister\MongoDb;

use As3\Modlr\Metadata\Interfaces\MergeableInterface;
use As3\Modlr\Metadata\Interfaces\StorageLayerInterface;

/**
 * Defines the MongoDB storage metadata for an entity (e.g. a database object).
 * Should be loaded using the MetadataFactory, not instantiated directly.
 *
 * @author Jacob Bare <jacob.bare@gmail.com>
 */
class StorageMetadata implements StorageLayerInterface
{
    /**
     * The database name.
     *
     * @var string
     */
    public $db;

    /**
     * The collection name.
     *
     * @var string
     */
    public $collection;

    /**
     * The ID strategy to use.
     * Currently object is the only valid choice.
     *
     * @todo Implement an auto-increment integer id strategy.
     * @var string
     */
    public $idStrategy = 'object';

    /**
     * Configured schemata for this entity
     *
     * @var array
     */
    public $schemata = [];

    /**
     * {@inheritDoc}
     */
    public function getKey()
    {
        return Persister::PERSISTER_KEY;
    }

    /**
     * {@inheritDoc}
     */
    public function merge(MergeableInterface $metadata)
    {
        return $this;
    }
}

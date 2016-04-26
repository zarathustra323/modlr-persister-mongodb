<?php

namespace As3\Modlr\Persister\MongoDb;

use As3\Modlr\Metadata\EntityMetadata;
use As3\Modlr\Persister\RecordSetInterface;
use As3\Modlr\Store\Store;
use Doctrine\MongoDB\Cursor;

/**
 * Represents records from MongoDb via a Cursor.
 *
 * @author  Jacob Bare <jacob.bare@gmail.com>
 */
class CursorRecordSet implements RecordSetInterface
{
    /**
     * @var Cursor
     */
    private $cursor;

    /**
     * @var Hydrator
     */
    private $hydrator;

    /**
     * @var EntityMetadata
     */
    private $metadata;

    /**
     * @var Store
     */
    private $store;

    /**
     * Constructor.
     *
     * @param   EntityMetadata  $metadata
     * @param   Cursor          $cursor
     * @param   Store           $store
     * @param   Hydrator        $hydrator
     */
    public function __construct(EntityMetadata $metadata, Cursor $cursor, Store $store, Hydrator $hydrator)
    {
        $this->metadata = $metadata;
        $this->cursor = $cursor;
        $this->hydrator = $hydrator;
        $this->store = $store;
    }

    /**
     * {@inheritdoc}
     */
    public function count()
    {
        return $this->cursor->count(true);
    }

    /**
     * {@inheritdoc}
     */
    public function current()
    {
        $current = $this->cursor->current();
        return $this->hydrator->normalize($this->metadata, $current, $this->store);
    }

    /**
     * {@inheritdoc}
     */
    public function key()
    {
        return $this->cursor->key();
    }

    /**
     * {@inheritdoc}
     */
    public function getSingleResult()
    {
        $record = $this->cursor->getSingleResult();
        if (!is_array($record)) {
            return;
        }
        return $this->hydrator->normalize($this->metadata, $record, $this->store);
    }

    /**
     * {@inheritdoc}
     */
    public function next()
    {
        $this->cursor->next();
    }

    /**
     * {@inheritdoc}
     */
    public function rewind()
    {
        $this->cursor->rewind();
    }

    /**
     * {@inheritdoc}
     */
    public function totalCount()
    {
        return $this->cursor->count(false);
    }

    /**
     * {@inheritdoc}
     */
    public function valid()
    {
        return $this->cursor->valid();
    }
}

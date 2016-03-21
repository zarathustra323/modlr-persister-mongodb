<?php

namespace As3\Modlr\Persister\MongoDb;

use As3\Modlr\Exception\MetadataException;
use As3\Modlr\Metadata\EntityMetadata;
use As3\Modlr\Metadata\Interfaces\StorageMetadataFactoryInterface;
use As3\Modlr\Util\EntityUtility;

/**
 * Creates MongoDb storage Metadata instances for use with metadata drivers.
 * Is also responsible for validating storage objects.
 *
 * @author Jacob Bare <jacob.bare@gmail.com>
 */
final class StorageMetadataFactory implements StorageMetadataFactoryInterface
{
    /**
     * @var EntityUtility
     */
    private $entityUtil;

    /**
     * Constructor.
     *
     * @param   EntityUtility   $entityUtil
     */
    public function __construct(EntityUtility $entityUtil)
    {
        $this->entityUtil = $entityUtil;
    }

    /**
     * {@inheritDoc}
     */
    public function createInstance(array $mapping)
    {
        $persistence = new StorageMetadata();

        if (isset($mapping['db'])) {
            $persistence->db = $mapping['db'];
        }

        if (isset($mapping['collection'])) {
            $persistence->collection = $mapping['collection'];
        }
        return $persistence;
    }

    /**
     * {@inheritDoc}
     */
    public function handleLoad(EntityMetadata $metadata)
    {
        if (null === $metadata->persistence->collection) {
            $metadata->persistence->collection = $metadata->type;
        }
    }

    /**
     * {@inheritDoc}
     */
    public function handleValidate(EntityMetadata $metadata)
    {
        $this->validateIdStrategy($metadata);
        $this->validateDatabase($metadata);
        $this->validateCollectionNaming($metadata);
    }



    /**
     * Validates that the collection naming is correct, based on entity format config.
     *
     * @param   EntityMetadata  $metadata
     * @throws  MetadataException
     */
    private function validateCollectionNaming(EntityMetadata $metadata)
    {
        $persistence = $metadata->persistence;
        if (false === $this->entityUtil->isEntityTypeValid($persistence->collection)) {
            throw MetadataException::invalidMetadata(
                $metadata->type,
                sprintf('The entity persistence collection "%s" is invalid based on the configured name format "%s"',
                        $persistence->collection,
                        $this->entityUtil->getRestConfig()->getEntityFormat()
                )
            );
        }
    }

    /**
     * Validates that the proper database properties are set.
     *
     * @param   EntityMetadata  $metadata
     * @throws  MetadataException
     */
    private function validateDatabase(EntityMetadata $metadata)
    {
        $persistence = $metadata->persistence;
        if (false === $metadata->isChildEntity() && (empty($persistence->db) || empty($persistence->collection))) {
            throw MetadataException::invalidMetadata($metadata->type, 'The persistence database and collection names cannot be empty.');
        }
    }

    /**
     * Validates the proper id strategy.
     *
     * @param   EntityMetadata  $metadata
     * @throws  MetadataException
     */
    private function validateIdStrategy(EntityMetadata $metadata)
    {
        $persistence = $metadata->persistence;
        $validIdStrategies = ['object'];
        if (!in_array($persistence->idStrategy, $validIdStrategies)) {
            throw MetadataException::invalidMetadata($metadata->type, sprintf('The persistence id strategy "%s" is invalid. Valid types are "%s"', $persistence->idStrategy, implode('", "', $validIdStrategies)));
        }
    }
}

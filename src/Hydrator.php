<?php

namespace As3\Modlr\Persister\MongoDb;

use As3\Modlr\Metadata\EntityMetadata;
use As3\Modlr\Metadata\RelationshipMetadata;
use As3\Modlr\Persister\PersisterException;
use As3\Modlr\Store\Store;
use Doctrine\MongoDB\Cursor;

/**
 * Hydrates raw, MongoDB array results into RecordSet instances.
 *
 * @author Jacob Bare <jacob.bare@gmail.com>
 */
final class Hydrator
{
    /**
     * Creates a cursor record set object from a MongoDB cursor.
     *
     * @param   EntityMetadata  $metadata
     * @param   Cursor          $cursor
     * @param   Store           $store
     * @return  CursorRecordSet
     */
    public function createCursorRecordSet(EntityMetadata $metadata, Cursor $cursor, Store $store)
    {
        return new CursorRecordSet($metadata, $cursor, $store, $this);
    }

    /**
     * Extracts the model type from a raw MongoDB record.
     *
     * @param   EntityMetadata  $metadata
     * @param   array           $record
     * @return  string
     * @throws  PersisterException
     */
    public function extractType(EntityMetadata $metadata, array $record)
    {
        if (false === $metadata->isPolymorphic()) {
            return $metadata->type;
        }
        return $this->extractField(Persister::POLYMORPHIC_KEY, $record);
    }

    /**
     * Processes a raw MongoDB record and normalizes it into a standard array.
     *
     * @param   EntityMetadata  $metadata
     * @param   array           $record
     * @param   Store           $store
     * @return  array
     */
    public function normalize(EntityMetadata $metadata, array $record, Store $store)
    {
        // Get the identifier and model type value from the raw record.
        list($identifier, $type) = $this->extractIdAndType($metadata, $record);

        // Reload the metadata in case a polymorphic type was found.
        $metadata = $store->getMetadataForType($type);

        // Convert relationships to the proper format.
        $record = $this->convertRelationships($metadata, $record);

        return [
            'identifier'    => (String) $identifier,
            'type'          => $type,
            'properties'    => $record,
        ];
    }

    /**
     * Converts the relationships on a raw result to the proper format.
     *
     * @param   EntityMetadata  $metadata
     * @param   array           &$data
     * @return  array
     */
    private function convertRelationships(EntityMetadata $metadata, array $data)
    {
        foreach ($metadata->getRelationships() as $key => $relMeta) {
            if (!isset($data[$key])) {
                continue;
            }
            if (true === $relMeta->isMany() && !is_array($data[$key])) {
                throw PersisterException::badRequest(sprintf('Relationship key "%s" is a reference many. Expected record data type of array, "%s" found on model "%s" for identifier "%s"', $key, gettype($data[$key]), $type, $identifier));
            }
            $references = $relMeta->isOne() ? [$data[$key]] : $data[$key];

            $extracted = [];
            foreach ($references as $reference) {
                $extracted[] =  $this->extractRelationship($relMeta, $reference);
            }
            $data[$key] = $relMeta->isOne() ? reset($extracted) : $extracted;
        }
        return $data;
    }

    /**
     * Extracts a root field from a raw MongoDB result.
     *
     * @param   string  $key
     * @param   array   $data
     * @return  mixed
     * @throws  PersisterException
     */
    private function extractField($key, array $data)
    {
        if (!isset($data[$key])) {
            throw PersisterException::badRequest(sprintf('Unable to extract a field value. The "%s" key was not found.', $key));
        }
        return $data[$key];
    }

    /**
     * Extracts the identifier and model type from a raw result and removes them from the source data.
     * Returns as a tuple.
     *
     * @param   EntityMetadata  $metadata
     * @param   array           &$data
     * @return  array
     * @throws  PersisterException
     */
    private function extractIdAndType(EntityMetadata $metadata, array &$data)
    {
        $identifier = $this->extractField(Persister::IDENTIFIER_KEY, $data);
        unset($data[Persister::IDENTIFIER_KEY]);

        $key = Persister::POLYMORPHIC_KEY;
        $type = $this->extractType($metadata, $data);
        if (isset($data[$key])) {
            unset($data[$key]);
        }
        return [$identifier, $type];
    }

    /**
     * Extracts a standard relationship array that the store expects from a raw MongoDB reference value.
     *
     * @param   RelationshipMetadata    $relMeta
     * @param   mixed                   $reference
     * @return  array
     * @throws  RuntimeException        If the relationship could not be extracted.
     */
    private function extractRelationship(RelationshipMetadata $relMeta, $reference)
    {
        $simple  = false === $relMeta->isPolymorphic();
        $idKey   = Persister::IDENTIFIER_KEY;
        $typeKey = Persister::POLYMORPHIC_KEY;

        if (true === $simple && is_array($reference) && isset($reference[$idKey])) {
            return [
                'id'    => $reference[$idKey],
                'type'  => $relMeta->getEntityType(),
            ];
        }

        if (true === $simple && !is_array($reference)) {
            return [
                'id'    => $reference,
                'type'  => $relMeta->getEntityType(),
            ];
        }

        if (false === $simple && is_array($reference) && isset($reference[$idKey]) && isset($reference[$typeKey])) {
            return [
                'id'    => $reference[$idKey],
                'type'  => $reference[$typeKey],
            ];
        }
        throw PersisterException::badRequest('Unable to extract a reference id.');
    }
}

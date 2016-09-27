<?php

namespace As3\Modlr\Persister\MongoDb;

use Doctrine\MongoDB\Collection;

/**
 * Handles requests to create or sync schema
 *
 * @author  Josh Worden <solocommand@gmail.com>
 * @todo    This should be updated to read indices from the metadata factory
 */
class SchemaManager
{
    /**
     * Applies schemata
     *
     * @param   Collection  $collection     The MongoDB Collection
     * @param   array       $index          Associative array containing index data
     */
    public function createSchemata(Collection $collection, array $schemata)
    {
        foreach ($schemata as $schema) {
            if (!isset($schema['keys']) || empty($schema['keys'])) {
                throw new \InvalidArgumentException('Cannot create an index with no keys defined.');
            }
            $schema['options']['background'] = true;
            $collection->ensureIndex($schema['keys'], $schema['options']);
        }
    }

    /**
     * Syncs schemata state
     * @todo    Implement
     *
     * @param   Collection  $collection     The MongoDB Collection
     * @param   array       $index              Associative array containing index data
     */
    public function syncSchemata(Collection $collection, array $schemata)
    {
        return false;
        // Remove all indices that match the expected format (modlr_md5hash) and are NOT present in $schemata

        // Loop over $schemata and force update or remove/add index. Ensure background:true
    }
}

<?php

namespace As3\Modlr\Persister\MongoDb;

use \Closure;
use \MongoId;
use As3\Modlr\Metadata\AttributeMetadata;
use As3\Modlr\Metadata\EntityMetadata;
use As3\Modlr\Metadata\RelationshipMetadata;
use As3\Modlr\Models\Model;
use As3\Modlr\Persister\PersisterException;
use As3\Modlr\Store\Store;

/**
 * Handles persistence formatting operations for MongoDB.
 * - Formats query criteria to proper keys and values.
 * - Formats attribute and relationships values for insertion to the db.
 * - Formats identifier values for insertion to the db.
 *
 * @author Jacob Bare <jacob.bare@gmail.com>
 */
final class Formatter
{
    /**
     * Query operators.
     * Organized into handling groups.
     *
     * @var array
     */
    private $ops = [
        'root'      => ['$and', '$or', '$nor'],
        'single'    => ['$eq', '$gt', '$gte', '$lt', '$lte', '$ne'],
        'multiple'  => ['$in', '$nin', '$all'],
        'recursive' => ['$not', '$elemMatch'],
        'ignore'    => ['$exists', '$type', '$mod', '$size', '$regex', '$text', '$where'],
    ];

    /**
     * Formats a set of query criteria for a Model.
     * Ensures the id and type fields are properly applied.
     * Ensures that values are properly converted to their database equivalents: e.g dates, mongo ids, etc.
     *
     * @param   EntityMetadata  $metadata
     * @param   Store           $store
     * @param   array           $criteria
     * @return  array
     */
    public function formatQuery(EntityMetadata $metadata, Store $store, array $criteria)
    {
        $formatted = [];
        foreach ($criteria as $key => $value) {

            if ($this->isOpType('root', $key) && is_array($value)) {
                foreach ($value as $subKey => $subValue) {
                    $formatted[$key][$subKey] = $this->formatQuery($metadata, $store, $subValue);
                }
                continue;
            }

            if ($this->isOperator($key) && is_array($value)) {
                $formatted[$key] = $this->formatQuery($metadata, $store, $value);
                continue;
            }

            list($key, $value) = $this->formatQueryElement($key, $value, $metadata, $store);
            $formatted[$key] = $value;
        }
        return $formatted;
    }

    /**
     * Prepares and formats an attribute value for proper insertion into the database.
     *
     * @param   AttributeMetadata   $attrMeta
     * @param   mixed               $value
     * @return  mixed
     */
    public function getAttributeDbValue(AttributeMetadata $attrMeta, $value)
    {
        // Handle data type conversion, if needed.
        if ('date' === $attrMeta->dataType && $value instanceof \DateTime) {
            return new \MongoDate($value->getTimestamp(), $value->format('u'));
        }
        return $value;
    }

    /**
     * Prepares and formats a has-one relationship model for proper insertion into the database.
     *
     * @param   RelationshipMetadata    $relMeta
     * @param   Model|null              $model
     * @return  mixed
     */
    public function getHasOneDbValue(RelationshipMetadata $relMeta, Model $model = null)
    {
        if (null === $model || true === $relMeta->isInverse) {
            return null;
        }
        return $this->createReference($relMeta, $model);
    }

    /**
     * Prepares and formats a has-many relationship model set for proper insertion into the database.
     *
     * @param   RelationshipMetadata    $relMeta
     * @param   Model[]|null            $models
     * @return  mixed
     */
    public function getHasManyDbValue(RelationshipMetadata $relMeta, array $models = null)
    {
        if (null === $models || true === $relMeta->isInverse) {
            return null;
        }
        $references = [];
        foreach ($models as $model) {
            $references[] = $this->createReference($relMeta, $model);
        }
        return empty($references) ? null : $references;
    }

    /**
     * {@inheritDoc}
     */
    public function getIdentifierDbValue($identifier, $strategy = null)
    {
        if (false === $this->isIdStrategySupported($strategy)) {
            throw PersisterException::nyi('ID conversion currently only supports an object strategy, or none at all.');
        }
        if ($identifier instanceof MongoId) {
            return $identifier;
        }
        return new MongoId($identifier);
    }

    /**
     * Gets all possible identifier field keys (internal and persistence layer).
     *
     * @return  array
     */
    public function getIdentifierFields()
    {
        return [Persister::IDENTIFIER_KEY, EntityMetadata::ID_KEY];
    }

    /**
     * Gets all possible model type keys (internal and persistence layer).
     *
     * @return  array
     */
    public function getTypeFields()
    {
        return [Persister::POLYMORPHIC_KEY, EntityMetadata::TYPE_KEY];
    }

    /**
     * Determines if a field key is an identifier.
     * Uses both the internal and persistence identifier keys.
     *
     * @param   string  $key
     * @return  bool
     */
    public function isIdentifierField($key)
    {
        return in_array($key, $this->getIdentifierFields());
    }

    /**
     * Determines if the provided id strategy is supported.
     *
     * @param   string|null     $strategy
     * @return  bool
     */
    public function isIdStrategySupported($strategy)
    {
        return (null === $strategy || 'object' === $strategy);
    }

    /**
     * Determines if a field key is a model type field.
     * Uses both the internal and persistence model type keys.
     *
     * @param   string  $key
     * @return  bool
     */
    public function isTypeField($key)
    {
        return in_array($key, $this->getTypeFields());
    }

    /**
     * Creates a reference for storage of a related model in the database
     *
     * @param   RelationshipMetadata    $relMeta
     * @param   Model                   $model
     * @return  mixed
     */
    private function createReference(RelationshipMetadata $relMeta, Model $model)
    {
        $reference = [];
        $identifier = $this->getIdentifierDbValue($model->getId());
        if (true === $relMeta->isPolymorphic()) {
            $reference[Persister::IDENTIFIER_KEY] = $identifier;
            $reference[Persister::TYPE_KEY] = $model->getType();
            return $reference;
        }
        return $identifier;
    }

    /**
     * Formats a query element and ensures the correct key and value are set.
     * Returns a tuple of the formatted key and value.
     *
     * @param   string          $key
     * @param   mixed           $value
     * @param   EntityMetadata  $metadata
     * @param   Store           $store
     * @return  array
     */
    private function formatQueryElement($key, $value, EntityMetadata $metadata, Store $store)
    {
        // Handle root fields: id or model type.
        if (null !== $result = $this->formatQueryElementRoot($key, $value, $metadata)) {
            return $result;
        }

        // Handle attributes.
        if (null !== $result = $this->formatQueryElementAttr($key, $value, $metadata, $store)) {
            return $result;
        }

        // Handle relationships.
        if (null !== $result = $this->formatQueryElementRel($key, $value, $metadata, $store)) {
            return $result;
        }

        // Handle dot notated fields.
        if (null !== $result = $this->formatQueryElementDotted($key, $value, $metadata, $store)) {
            return $result;
        }

        // Pass remaining elements unconverted.
        return [$key, $value];
    }

    /**
     * Formats an attribute query element.
     * Returns a tuple of the formatted key and value, or null if the key is not an attribute field.
     *
     * @param   string          $key
     * @param   mixed           $value
     * @param   EntityMetadata  $metadata
     * @param   Store           $store
     * @return  array|null
     */
    private function formatQueryElementAttr($key, $value, EntityMetadata $metadata, Store $store)
    {
        if (null === $attrMeta = $metadata->getAttribute($key)) {
            return;
        }

        $converter = $this->getQueryAttrConverter($store, $attrMeta);

        if (is_array($value)) {

            if (true === $this->hasOperators($value)) {
                return [$key, $this->formatQueryExpression($value, $converter)];
            }

            if (in_array($attrMeta->dataType, ['array', 'object'])) {
                return [$key, $value];
            }
            return [$key, $this->formatQueryExpression(['$in' => $value], $converter)];
        }
        return [$key, $converter($value)];
    }

    /**
     * Formats a dot-notated field.
     * Returns a tuple of the formatted key and value, or null if the key is not a dot-notated field, or cannot be handled.
     *
     * @param   string          $key
     * @param   mixed           $value
     * @param   EntityMetadata  $metadata
     * @param   Store           $store
     * @return  array|null
     */
    private function formatQueryElementDotted($key, $value, EntityMetadata $metadata, Store $store)
    {
        if (false === stripos($key, '.')) {
            return;
        }

        $parts = explode('.', $key);
        $root = array_shift($parts);
        if (false === $metadata->hasRelationship($root)) {
            // Nothing to format. Allow the dotted field to pass normally.
            return [$key, $value];
        }
        $hasIndex = is_numeric($parts[0]);

        if (true === $hasIndex) {
            $subKey = isset($parts[1]) ? $parts[1] : 'id';
        } else {
            $subKey = $parts[0];
        }

        if ($this->isIdentifierField($subKey)) {
            // Handle like a regular relationship
            list($key, $value) = $this->formatQueryElementRel($root, $value, $metadata, $store);
            $key = (true === $hasIndex) ? sprintf('%s.%s', $key, $parts[0]) : $key;
            return [$key, $value];
        }

        if ($this->isTypeField($subKey)) {
            // Handle as a model type field.
            list($key, $value) = $this->formatQueryElementType($subKey, $value);
            $key = (true === $hasIndex) ? sprintf('%s.%s.%s', $root, $parts[0], $key) : sprintf('%s.%s', $root, $key);
            return [$key, $value];
        }
        return [$key, $value];
    }

    /**
     * Formats a relationship query element.
     * Returns a tuple of the formatted key and value, or null if the key is not a relationship field.
     *
     * @param   string          $key
     * @param   mixed           $value
     * @param   EntityMetadata  $metadata
     * @param   Store           $store
     * @return  array|null
     */
    private function formatQueryElementRel($key, $value, EntityMetadata $metadata, Store $store)
    {
        if (null === $relMeta = $metadata->getRelationship($key)) {
            return;
        }

        $converter = $this->getQueryRelConverter($store, $relMeta);

        if (true === $relMeta->isPolymorphic()) {
            $key = sprintf('%s.%s', $key, Persister::IDENTIFIER_KEY);
        }

        if (is_array($value)) {
            $value = (true === $this->hasOperators($value)) ? $value : ['$in' => $value];
            return [$key, $this->formatQueryExpression($value, $converter)];
        }
        return [$key, $converter($value)];
    }


    /**
     * Formats a root query element: either id or model type.
     * Returns a tuple of the formatted key and value, or null if the key is not a root field.
     *
     * @param   string          $key
     * @param   mixed           $value
     * @param   EntityMetadata  $metadata
     * @return  array|null
     */
    private function formatQueryElementRoot($key, $value, EntityMetadata $metadata)
    {
        if (true === $this->isIdentifierField($key)) {
            $dbKey = Persister::IDENTIFIER_KEY;
        } elseif (true === $this->isTypeField($key)) {
            $dbKey = Persister::POLYMORPHIC_KEY;
        } else {
            return;
        }

        $converter = $this->getQueryRootConverter($metadata, $dbKey);
        if (is_array($value)) {
            $value = (true === $this->hasOperators($value)) ? $value : ['$in' => $value];
            return [$dbKey, $this->formatQueryExpression($value, $converter)];
        }
        return [$dbKey, $converter($value)];
    }

    /**
     * Formats a query expression.
     *
     * @param   array   $expression
     * @param   Closure $converter
     * @return  array
     */
    private function formatQueryExpression(array $expression, Closure $converter)
    {
        foreach ($expression as $key => $value) {

            if (true === $this->isOpType('ignore', $key)) {
                continue;
            }

            if (true === $this->isOpType('single', $key)) {
                $expression[$key] = $converter($value);
                continue;
            }

            if (true === $this->isOpType('multiple', $key)) {
                $value = (array) $value;
                foreach ($value as $subKey => $subValue) {
                    $expression[$key][$subKey] = $converter($subValue);
                }
                continue;
            }

            if (true === $this->isOpType('recursive', $key)) {
                $value = (array) $value;
                $expression[$key] = $this->formatQueryExpression($value, $converter);
                continue;
            }

            $expression[$key] = $converter($value);
        }
        return $expression;
    }

    /**
     * Gets the converter for handling attribute values in queries.
     *
     * @param   Store               $store
     * @param   AttributeMetadata   $attrMeta
     * @return  Closure
     */
    private function getQueryAttrConverter(Store $store, AttributeMetadata $attrMeta)
    {
        return function ($value) use ($store, $attrMeta) {
            $value = $store->convertAttributeValue($attrMeta->dataType, $value);
            return $this->getAttributeDbValue($attrMeta, $value);
        };

    }

    /**
     * Gets the converter for handling relationship values in queries.
     *
     * @param   Store                   $store
     * @param   RelationshipMetadata    $relMeta
     * @return  Closure
     */
    private function getQueryRelConverter(Store $store, RelationshipMetadata $relMeta)
    {
        $related = $store->getMetadataForType($relMeta->getEntityType());
        return $this->getQueryRootConverter($related, $related->getKey());
    }

    /**
     * Gets the converter for handling root field values in queries (id or model type).
     *
     * @param   EntityMetadata  $metadata
     * @param   string          $key
     * @return  Closure
     */
    private function getQueryRootConverter(EntityMetadata $metadata, $key)
    {
        return function($value) use ($metadata, $key) {
            if ($key === Persister::POLYMORPHIC_KEY) {
                return $value;
            }
            $strategy = ($metadata->persistence instanceof StorageMetadata) ? $metadata->persistence->idStrategy : null;
            return $this->getIdentifierDbValue($value, $strategy);
        };
    }

    /**
     * Determines whether a query value has additional query operators.
     *
     * @param   mixed   $value
     * @return  bool
     */
    private function hasOperators($value)
    {

        if (!is_array($value) && !is_object($value)) {
            return false;
        }

        if (is_object($value)) {
            $value = get_object_vars($value);
        }

        foreach ($value as $key => $subValue) {
            if (true === $this->isOperator($key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines if a key is a query operator.
     *
     * @param   string  $key
     * @return  bool
     */
    private function isOperator($key)
    {
        return isset($key[0]) && '$' === $key[0];
    }

    /**
     * Determines if a key is of a certain operator handling type.
     *
     * @param   string  $type
     * @param   string  $key
     * @return  bool
     */
    private function isOpType($type, $key)
    {
        if (!isset($this->ops[$type])) {
            return false;
        }
        return in_array($key, $this->ops[$type]);
    }
}

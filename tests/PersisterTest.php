<?php

namespace As3\Modlr\Persister\MongoDb\Tests;

use PHPUnit_Framework_TestCase;
use Doctrine\MongoDB\Configuration;
use Doctrine\MongoDB\Connection;
use As3\Modlr;

class PersisterTest extends PHPUnit_Framework_TestCase
{
    protected static $dbName = 'modlr_mongodb';

    protected $connection;
    protected $smf;
    protected $persister;

    public function setUp()
    {
        // Initialize the doctrine connection
        $config = new Configuration();
        $config->setLoggerCallable(function($msg) {});
        $this->connection = new Connection(null, array(), $config);

        // Initialize the metadata factory
        $typeFactory = new Modlr\DataTypes\TypeFactory;
        $validator = new Modlr\Util\Validator;

        $config = new Modlr\Rest\RestConfiguration($validator);
        $config->setRootEndpoint('/modlr/api/');

        $entityUtil = new Modlr\Util\EntityUtility($config, $typeFactory);
        $this->smf = new Modlr\Persister\MongoDb\StorageMetadataFactory($entityUtil);

        $formatter = new Modlr\Persister\MongoDb\Formatter();
        $query = new Modlr\Persister\MongoDb\Query($this->connection, $formatter);

        $hydrator = new Modlr\Persister\MongoDb\Hydrator();
        $schemaManager = new Modlr\Persister\MongoDb\SchemaManager();

        $this->persister = new Modlr\Persister\MongoDb\Persister($query, $this->smf, $hydrator, $schemaManager);
    }

    public function tearDown()
    {
        $collections = $this->connection->selectDatabase(self::$dbName)->listCollections();
        foreach ($collections as $collection) {
            $collection->drop();
        }

        $this->connection->close();
        unset($this->connection);
    }

    public function testPersisterValues()
    {
        $this->assertEquals('mongodb', $this->persister->getPersisterKey());
        $this->assertEquals('_type', $this->persister->getPolymorphicKey());
        $this->assertEquals('_id', $this->persister->getIdentifierKey());
    }

    public function testMDFInstance()
    {
        $this->assertInstanceOf(
            'As3\Modlr\Metadata\Interfaces\StorageMetadataFactoryInterface',
            $this->persister->getPersistenceMetadataFactory()
        );
    }

    /**
     * @expectedException           As3\Modlr\Persister\PersisterException
     * @expectedExceptionMessage    ID conversion currently only supports an object strategy, or none at all.
     */
    public function testConvertIdInvalidStrategy()
    {
        $this->assertInstanceOf('MongoId', $this->persister->convertId('test', 'blag'));
    }

    /**
     * @expectedException           As3\Modlr\Persister\PersisterException
     * @expectedExceptionMessage    ID generation currently only supports an object strategy, or none at all.
     */
    public function testGenerateIdInvalidStrategy()
    {
        $this->assertInstanceOf('MongoId', $this->persister->generateId('blag'));
    }

    public function testConvertIdObject()
    {
        $test = '49a7011a05c677b9a916612a';
        $id = new \MongoId($test);

        $this->assertEquals($id, $this->persister->convertId($id));
        $this->assertEquals($id, $this->persister->convertId($id, 'object'));
    }

    public function testGenerateIdObject()
    {
        $this->assertInstanceOf('MongoId', $this->persister->generateId());
        $this->assertInstanceOf('MongoId', $this->persister->generateId('object'));
    }

    /**
     * @expectedException           As3\Modlr\Persister\PersisterException
     * @expectedExceptionMessage    ID conversion currently only supports an object strategy, or none at all.
     */
    public function testConvertIdIncrementId()
    {
        $test = 12345;
        $this->assertEquals($test, $this->persister->convertId($test, 'incrementId'));
    }

    /**
     * @expectedException           As3\Modlr\Persister\PersisterException
     * @expectedExceptionMessage    ID generation currently only supports an object strategy, or none at all.
     */
    public function testGenerateIdIncrementId()
    {
        $val = $this->persister->generateId('incrementId');
        $this->assertGreaterThan($val, $this->persister->generateId('incrementId'));
    }

    public function testNameIsAppendedToSchemata()
    {
        $metadata = $this->getMetadata()->persistence;

        foreach ($metadata->schemata as $schema) {
            $this->assertTrue(isset($schema['options']['name']) && !empty($schema['options']['name']), 'index name was not applied to schema');
            $this->assertSame(stripos($schema['options']['name'], 'modlr_'), 0, '`modlr_` prefix missing from schema name');
        }
    }

    /**
     * @expectedException           As3\Modlr\Exception\MetadataException
     * @expectedExceptionMessage    At least one key must be specified to define an index.
     */
    public function testSchemaRequiresKeys()
    {
        $schemata = [['options' => ['unique' => true]]];
        $metadata = $this->getMetadata($schemata);
    }

    /**
     * @expectedException           As3\Modlr\Exception\MetadataException
     * @expectedExceptionMessage    At least one key must be specified to define an index.
     */
    public function testSchemaRequiresAtLeastOneKey()
    {
        $schemata = ['keys' => [], ['options' => ['unique' => true]]];
        $metadata = $this->getMetadata($schemata);
    }

    /**
     * @expectedException           InvalidArgumentException
     * @expectedExceptionMessage    Cannot create an index with no keys defined.
     */
    public function testSchemaCreateRequiresKeys()
    {
        $collection = $this->connection->selectCollection(self::$dbName, 'test-model');
        $manager = new Modlr\Persister\MongoDb\SchemaManager;

        $schemata = [
            ['options' => ['unique' => true]]
        ];

        $manager->createSchemata($collection, $schemata);
    }

    /**
     * @expectedException           InvalidArgumentException
     * @expectedExceptionMessage    Cannot create an index with no keys defined.
     */
    public function testSchemaCreateRequiresAtLeastOneKey()
    {
        $collection = $this->connection->selectCollection(self::$dbName, 'test-model');
        $manager = new Modlr\Persister\MongoDb\SchemaManager;

        $schemata = [
            ['keys' => [], 'options' => ['unique' => true]]
        ];

        $manager->createSchemata($collection, $schemata);
    }

    public function testSchemaCreation()
    {
        $metadata = $this->getMetadata();
        $this->persister->createSchemata($metadata);

        $collection = $this->connection->selectCollection(self::$dbName, 'test-model');
        $indices = $collection->getIndexInfo();

        foreach ($metadata->persistence->schemata as $schema) {
            $found = false;
            foreach ($indices as $index) {
                if ($index['name'] === $schema['options']['name']) {
                    $found = true;
                }
            }
            $this->assertTrue($found, sprintf('Index for "%s" was not created!', $schema['options']['name']));
        }
    }

    private function getMetadata(array $schemata = [])
    {
        $mapping = [
            'type'          => 'test-model',
            'attributes'    => [
                'name'          => ['data_type' => 'string'],
                'active'        => ['data_type' => 'boolean']
            ],
            'persistence'   => [
                'db'            => self::$dbName,
                'collection'    => 'test-model',
                'schemata'      => [
                    ['keys' => ['name' => 1]],
                    ['keys' => ['active' => 1], ['options' => ['unique' => true]]]
                ]
            ]
        ];
        if (!empty($schemata)) {
            $mapping['persistence']['schemata'] = $schemata;
        }
        $metadata = new Modlr\Metadata\EntityMetadata($mapping['type']);
        $pmd = $this->smf->createInstance($mapping['persistence']);
        $metadata->setPersistence($pmd);
        $this->smf->handleValidate($metadata);
        return $metadata;
    }
}

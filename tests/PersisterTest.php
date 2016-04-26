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

        $this->persister = new Modlr\Persister\MongoDb\Persister($query, $this->smf, $hydrator);
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
}

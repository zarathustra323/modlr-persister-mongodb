<?php

namespace As3\Modlr\Persister\MongoDb;

use Psr\Log\LoggerInterface;

/**
 * Implements query logging support for the MongoDB Persister
 *
 * @author Josh Worden <solocommand@gmail.com>
 */
class Logger
{
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var string
     */
    private $prefix;

    /**
     * @var int
     */
    private $batchInsertThreshold = 4;

    /**
     * DI Constructor
     *
     * @param   LoggerInterface  $logger
     * @param   string      $prefix
     */
    public function __construct(LoggerInterface $logger, $prefix = 'MongoDB query', $batchThreshold = 4)
    {
        $this->logger = $logger;
        $this->prefix = $prefix;
        $this->batchInsertThreshold = $batchThreshold;
    }

    /**
     * Logs a mongodb query
     *
     * @param   array       $query  The query to log
     */
    public function logQuery(array $query)
    {
        if (isset($query['batchInsert']) && null !== $this->batchInsertThreshold && $this->batchInsertThreshold <= $query['num']) {
            $query['data'] = '**'.$query['num'].' item(s)**';
        }

        array_walk_recursive($query, function(&$value, $key) {
            if ($value instanceof \MongoBinData) {
                $value = base64_encode($value->bin);
                return;
            }
            if (is_float($value) && is_infinite($value)) {
                $value = ($value < 0 ? '-' : '') . 'Infinity';
                return;
            }
            if (is_float($value) && is_nan($value)) {
                $value = 'NaN';
                return;
            }
        });

        $this->logger->info($this->prefix, $query);
    }
}

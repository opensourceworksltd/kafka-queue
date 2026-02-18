<?php

namespace KafkaQueue;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    public function __construct(protected $consumer, protected $producer) {}

    public function size($queue = null) {}

    public function push($job, $data = "", $queue = null)
    {
        $topicObj = $this->producer->newTopic($queue ?? env("KAFKA_TOPIC",'default'));
        $topicObj->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
    }
    public function pushRaw($payload, $queue = null, array $options = []) {}
    public function later($delay, $job, $data = "", $queue = null) {}
    public function pop($queue = null) {
         $this->consumer->subscribe([$queue]);

          try {
            $message = $this->consumer->consume(120 * 1000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages\n will wait for more\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("Timed out\n");
                    break;
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                default:
                    throw new \Exception($message->errstr, $message->err);
                    break;
            }
        }catch (\Exception $e) {
            var_dump($e->getMessage()); 
        }
    }
}

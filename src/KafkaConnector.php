<?php

namespace KafkaQueue;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $conf = new \RdKafka\Conf();
        $conf->set("security.protocol", $config['security_protocol']);
        $conf->set("sasl.mechanisms", $config['sasl_mechanisms']);
        $conf->set("sasl.username", $config['sasl_username']);
        $conf->set("sasl.password", $config['sasl_password']);
        $conf->set(
            'bootstrap.servers',
            $config['bootstrap_servers']
        );
        $conf->set('ssl.ca.location', '/etc/ssl/cert.pem');

        $conf->set("group.id", $config['group_id']);
        $conf->set("auto.offset.reset", "earliest");
        
        // create consumer
        $consumer = new \RdKafka\KafkaConsumer($conf);
        
 

        $producer = new \RdKafka\Producer($conf);
       

        return new KafkaQueue($consumer, $producer);
    }
}

<?php

namespace KafkaQueue;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        // Configuration for both producer and consumer (common settings)
        $baseConf = new \RdKafka\Conf();
        $baseConf->set("security.protocol", $config['security_protocol']);
        $baseConf->set("sasl.mechanisms", $config['sasl_mechanisms']);
        $baseConf->set("sasl.username", $config['sasl_username']);
        $baseConf->set("sasl.password", $config['sasl_password']);
        $baseConf->set(
            'bootstrap.servers',
            $config['bootstrap_servers']
        );
        $baseConf->set('ssl.ca.location', '/etc/ssl/cert.pem');

        // Producer configuration (without consumer-specific properties)
        $producerConf = new \RdKafka\Conf();
        $producerConf->set("security.protocol", $config['security_protocol']);
        $producerConf->set("sasl.mechanisms", $config['sasl_mechanisms']);
        $producerConf->set("sasl.username", $config['sasl_username']);
        $producerConf->set("sasl.password", $config['sasl_password']);
        $producerConf->set(
            'bootstrap.servers',
            $config['bootstrap_servers']
        );
        $producerConf->set('ssl.ca.location', '/etc/ssl/cert.pem');

        // Consumer configuration (with consumer-specific properties)
        $consumerConf = new \RdKafka\Conf();
        $consumerConf->set("security.protocol", $config['security_protocol']);
        $consumerConf->set("sasl.mechanisms", $config['sasl_mechanisms']);
        $consumerConf->set("sasl.username", $config['sasl_username']);
        $consumerConf->set("sasl.password", $config['sasl_password']);
        $consumerConf->set(
            'bootstrap.servers',
            $config['bootstrap_servers']
        );
        $consumerConf->set('ssl.ca.location', '/etc/ssl/cert.pem');

        // Consumer-specific properties
        $consumerConf->set("group.id", $config['group_id']);
        $consumerConf->set("auto.offset.reset", "earliest");

        // create consumer
        $consumer = new \RdKafka\KafkaConsumer($consumerConf);

        // create producer
        $producer = new \RdKafka\Producer($producerConf);


        return new KafkaQueue($consumer, $producer);
    }
}

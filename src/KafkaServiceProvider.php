<?php

namespace KafkaQueue;

use App\Connector\KafkaConnector;
use Illuminate\Support\ServiceProvider;

class KafkaServiceProvider extends ServiceProvider
{
    /**
     * Register services.
     */
    public function register(): void
    {
        //
    }

    /**
     * Bootstrap services.
     */
    public function boot(): void
    {
        $this->app['queue']->addConnector('kafka', function () {
            return new KafkaConnector();
        });
    }
}

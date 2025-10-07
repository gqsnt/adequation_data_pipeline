<?php

namespace App\Services;

use Illuminate\Http\Client\RequestException;
use Illuminate\Support\Facades\Http;
use App\Models\{Project, Source, Mapping, Dataset};

class WorkerClient
{
    private string $base;

    public function __construct()
    {
        $this->base = rtrim(config('services.ETL.url'), '/');
    }

    /**
     * @throws RequestException
     */
    public function infer_schema(array $payload): array {
        $path = "{$this->base}/infer_schema";
        return Http::post($path, $payload)->throw()->json();
    }

    /**
     * @throws RequestException
     */
    public function run(array $payload): array {
        return Http::post("{$this->base}/run", $payload)->throw()->json();
    }

}

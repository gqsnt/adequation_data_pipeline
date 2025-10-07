<?php

namespace App\Services;

use App\Models\Project;

final class Warehouse {
    public static function uri(Project $project): string
    {
        // Priorité au projet, fallback config
        return $project->warehouse_uri ?: config('services.WAREHOUSE.uri');
    }

    // Helpers symboliques (les noms réels sont passés au worker)
    public static function bronzeTable(): string      { return 'bronze'; }
    public static function silverTable(): string      { return 'silver'; }
    public static function quarantineTable(): string  { return 'silver_rejects'; }
}

<?php

namespace Database\Seeders;

use App\Models\Project;
use App\Models\Source;
use App\Models\User;
// use Illuminate\Database\Console\Seeds\WithoutModelEvents;
use Illuminate\Database\Seeder;

class DatabaseSeeder extends Seeder
{
    /**
     * Seed the application's database.
     */
    public function run(): void
    {
        // User::factory(10)->create();

        User::factory()->create([
            'name' => 'Test User',
            'email' => 'test@example.com',
        ]);

        $project = Project::create([
            'slug' => 'dvf',
            'warehouse_uri' => "file:///var/data/warehouse",
            'namespace' => "dvf",
        ]);

        Source::create([
            "project_id" => $project->id,
            "name" => "dvf_2020_2024_csv",
            "uri" => "file:///samples/dvf.csv",
            "config" => [
                "Csv" => [
                    "delimiter" => ",",
                    "has_header" => true,
                    "encoding" => "utf-8",
                ]
            ],
        ]);




    }
}

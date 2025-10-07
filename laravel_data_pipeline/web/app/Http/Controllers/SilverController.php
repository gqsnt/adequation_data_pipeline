<?php

namespace App\Http\Controllers;

use App\Models\Project;
use App\Models\Source;
use Illuminate\Http\Request;

class SilverController extends Controller
{
    public function show(Project $project)
    {
        return redirect()->route('projects.show', $project);
    }

    // PUT /{project}/silver
    public function update(Project $project, Request $r)
    {
        $v = $r->validate([
            'target_schema' => 'required|array|min:1',
            'target_schema.*.name' => 'required|string',
            'target_schema.*.type' => 'required|string',
            'target_schema.*.nullable' => 'boolean',
            'primary_key' => 'required|array|min:1',
        ]);

        $ds = $project->datasets()->firstOrNew(['layer' => 'silver', 'name' => 'silver']);
        $ds->schema = $v['target_schema'];
        $ds->primary_key = array_values($v['primary_key']);
        $ds->save();

        return redirect()->route('projects.show', $project)->with('success', 'Silver schema saved.');
    }

    // POST /{project}/silver/seed-from-source/{source}
    public function seedFromSource(Project $project, Source $source)
    {
        abort_unless($source->project_id === $project->id, 404);

        $bronze = $project->datasets()
            ->where(['layer' => 'bronze', 'source_id' => $source->id, 'name' => $source->name])
            ->firstOrFail();

        $fields = collect($bronze->schema ?? [])
            ->map(function ($f) {
                return [
                    'name' => $f['name'],
                    'type' => $this->canonType($f['type'] ?? 'str'),
                    'nullable' => true,
                ];
            })->values()->all();

        $ds = $project->datasets()->firstOrNew(['layer' => 'silver', 'name' => 'silver']);
        $ds->schema = $fields;
        // la PK reste à définir côté UI
        $ds->primary_key = $ds->primary_key ?? [];
        $ds->save();

        return redirect()->route('projects.show', $project)->with('success', 'Silver schema seeded from source.');
    }

    private function canonType(string $t): string
    {
        $x = strtolower($t);
        if (in_array($x, ['utf8','str','string'])) return 'str';
        if (in_array($x, ['f64','double','f32','float','float32','float64'])) return 'f64';
        if (in_array($x, ['i64','int64','i32','int32'])) return 'i64';
        if (in_array($x, ['bool','boolean'])) return 'bool';
        if (in_array($x, ['date','date32'])) return 'date';
        if (in_array($x, ['datetime','timestamp'])) return 'datetime';
        return 'str';
    }
}

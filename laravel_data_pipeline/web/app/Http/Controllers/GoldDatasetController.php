<?php

namespace App\Http\Controllers;

use App\Models\Dataset;
use App\Models\Project;
use App\Services\WorkerClient;
use Illuminate\Http\Request;

class GoldDatasetController extends Controller
{
    public function store(Project $project, Request $request)
    {
        $v = $request->validate([
            'name' => 'required|string',
            'schema' => 'required|array|min:1',
            'schema.*.name' => 'required|string',
            'schema.*.type' => 'required|string',
            'schema.*.nullable' => 'boolean',
            'primary_key' => 'nullable|array',
        ]);

        // Unicité par projet/layer/name est déjà imposée en DB si vous gardez l’index unique.
        $ds = $project->datasets()->create([
            'name'        => $v['name'],
            'layer'       => 'gold',
            'schema'      => $v['schema'],
            'primary_key' => $v['primary_key'] ?? [],
            'source_id'   => null,
        ]);

        return back()->with('success', 'Gold dataset created')->with('created_gold_id', $ds->id);
    }

    public function update(Project $project, Dataset $dataset, Request $request)
    {
        abort_unless($dataset->project_id === $project->id && $dataset->layer === 'gold', 404);

        $v = $request->validate([
            'name' => 'sometimes|string',
            'schema' => 'sometimes|array|min:1',
            'schema.*.name' => 'required_with:schema|string',
            'schema.*.type' => 'required_with:schema|string',
            'schema.*.nullable' => 'boolean',
            'primary_key' => 'sometimes|array',
        ]);
        if (isset($v['primary_key'])) {
            $v['primary_key'] = array_values($v['primary_key']);
        }

        $dataset->update($v);

        return back()->with('success', 'Gold dataset updated');
    }

    public function destroy(Project $project, Dataset $dataset)
    {
        abort_unless($dataset->project_id === $project->id && $dataset->layer === 'gold', 404);

        $dataset->delete();

        return back()->with('success', 'Gold dataset deleted');
    }
}

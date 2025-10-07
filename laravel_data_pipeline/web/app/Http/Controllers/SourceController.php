<?php

namespace App\Http\Controllers;

use App\Models\Project;
use App\Models\Source;
use App\Services\WorkerClient;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

class SourceController extends Controller
{
    public function store(Project $project, Request $r)
    {
        $v = $r->validate([
            'name' => 'required|string',
            'uri' => 'required|string',
            'config' => 'required|array',
        ]);

        $project->sources()->create($v);
        return redirect()->route('projects.show', $project)->with('ok', 'Source créée');
    }

    public function update(Project $project, Source $source, Request $r)
    {
        abort_unless($source->project_id === $project->id, 404);

        $v = $r->validate([
            'name' => 'required|string',
            'uri' => 'required|string',
            'config' => 'required|array',
        ]);

        $source->update($v);
        return redirect()->route('projects.show', $project)->with('ok', 'Source mise à jour');
    }

    public function destroy(Project $project, Source $source)
    {
        abort_unless($source->project_id === $project->id, 404);
        $source->delete();
        return redirect()->route('projects.show', $project)->with('ok', 'Source supprimée');
    }

    // NEW: /{project}/sources/{source}/infer_schema
    public function infer_schema(Project $project, Source $source, Request $r)
    {
        abort_unless($source->project_id === $project->id, 404);

        $limit = min(1000, max(1, (int)$r->get('limit', 200)));
        $payload = [
            'uri' => $source->uri,
            'source_config' => $source->config,
            'limit' => $limit,
        ];

        $resp = (new WorkerClient)->infer_schema($payload);
        if ($resp && ($resp['schema'] ?? null)) {
            $bronze = $project->datasets()->where([
                'layer' => 'bronze',
                'source_id' => $source->id,
                'name' => $source->name,
            ])->first();

            if ($bronze) {
                $bronze->update(['schema' => $resp['schema']['fields']]);
            } else {
                $project->datasets()->create([
                    'layer' => 'bronze',
                    'name' => $source->name,
                    'source_id' => $source->id,
                    'schema' => $resp['schema']['fields'],
                    'primary_key' => [], // bronze: pas de PK
                ]);
            }
        }

        return redirect()->route('projects.show', $project);
    }
}

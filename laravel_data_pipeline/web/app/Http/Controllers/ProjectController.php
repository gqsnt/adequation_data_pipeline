<?php

namespace App\Http\Controllers;

use App\Models\Project;
use Inertia\Inertia;
use Illuminate\Http\Request;

class ProjectController extends Controller
{
    public function index()
    {
        return Inertia::render('Projects/Index', [
            'projects' => Project::query()->orderByDesc('created_at')->get(),
        ]);
    }

    public function store(Request $r)
    {
        $p = Project::create($r->validate([
            'slug' => 'required|string|unique:projects,slug',
            'warehouse_uri' => 'required|string',
            'namespace' => 'required|string',
        ]));

        return redirect()->route('projects.show', $p);
    }

    public function show(Project $project)
    {
        $project->load([
            'sources:id,project_id,name,uri,config',
            'datasets:id,project_id,layer,name,schema,primary_key,source_id',
            'mappings:id,project_id,from_dataset_id,to_dataset_id,transforms,dq_rules',
            'pipelines:id,project_id,name,mapping_silver_id,mapping_gold_id',
            'pipelines.mapSilver:id,project_id,from_dataset_id,to_dataset_id,transforms,dq_rules',
            'pipelines.mapGold:id,project_id,from_dataset_id,to_dataset_id,transforms,dq_rules',
            'runs:id,project_id,pipeline_id,state,state_reason,rows_source,rows_source_rejected,rows_silver,rows_silver_rejected,rows_gold,bronze_snapshot,silver_snapshot,gold_snapshot,started_at,finished_at,dq_summary,logs',
            'runs.pipeline:id,project_id,name',
            'runs.errors:id,project_id,run_id,reason_code,message,row_no,source_values',
        ]);

        // Silver unique (facilité pour le front)
        $silver = $project->datasets->firstWhere('layer', 'silver');

        return Inertia::render('Projects/Show', [
            'project' => [
                'id' => (string)$project->id,
                'namespace' => $project->namespace,
                'warehouse_uri' => $project->warehouse_uri,
                'sources' => $project->sources->values(),
                'datasets' => $project->datasets->map(fn($d) => [
                    'id'          => $d->id,
                    'project_id'  => (string)$d->project_id,
                    'name'        => $d->name,
                    'layer'       => $d->layer,
                    'schema'      => $d->schema,
                    'primary_key' => $d->primary_key,
                    'source_id'   => $d->source_id,
                ])->values(),
                'mappings' => $project->mappings->values(),
                'pipelines' => $project->pipelines->values(),
                'runs' => $project->runs->map(function ($r) {
                    // joindre pipeline minimal pour affichage
                    $r->setRelation('pipeline', $r->pipeline ? $r->pipeline->only(['id','name']) : null);
                    return $r;
                })->values(),
                'silver' => $silver ? [
                    'id' => $silver->id,
                    'schema' => $silver->schema,
                    'primary_key' => $silver->primary_key,
                ] : null,
            ],
        ]);
    }
}

<?php

namespace App\Http\Controllers;

use App\Models\Mapping;
use App\Models\Project;
use Illuminate\Http\Request;

class MappingController extends Controller
{
    public function store(Project $project, Request $r)
    {
        $v = $r->validate([
            'from_dataset_id' => 'required|integer|exists:datasets,id',
            'to_dataset_id'   => 'required|integer|exists:datasets,id',
            'transforms'      => 'required|array|min:1',
            'dq_rules'        => 'sometimes|array',
        ]);

        $from = $project->datasets()->findOrFail($v['from_dataset_id']);
        $to   = $project->datasets()->findOrFail($v['to_dataset_id']);

        // Enforcement transitions: bronze->silver ou silver->gold
        $pair = $from->layer.'->'.$to->layer;
        abort_unless(in_array($pair, ['bronze->silver','silver->gold']), 422);

        // Upsert unique par (project, from, to)
        $mapping = $project->mappings()->updateOrCreate(
            ['from_dataset_id' => $from->id, 'to_dataset_id' => $to->id],
            ['transforms' => $v['transforms'], 'dq_rules' => $v['dq_rules'] ?? []],
        );

        return redirect()->route('projects.show', $project)->with('success', 'Mapping enregistré');
    }

    public function update(Project $project, Mapping $mapping, Request $r)
    {
        abort_unless($mapping->project_id === $project->id, 404);

        $v = $r->validate([
            'from_dataset_id' => 'sometimes|integer|exists:datasets,id',
            'to_dataset_id'   => 'sometimes|integer|exists:datasets,id',
            'transforms'      => 'sometimes|array|min:1',
            'dq_rules'        => 'sometimes|array',
        ]);

        if (isset($v['from_dataset_id'])) {
            $from = $project->datasets()->findOrFail($v['from_dataset_id']);
            $mapping->from_dataset_id = $from->id;
        }
        if (isset($v['to_dataset_id'])) {
            $to = $project->datasets()->findOrFail($v['to_dataset_id']);
            $mapping->to_dataset_id = $to->id;
        }

        // Re-enforce transition si modifiée
        $fromLayer = $mapping->from_dataset()->firstOrFail()->layer;
        $toLayer   = $mapping->to_dataset()->firstOrFail()->layer;
        $pair = $fromLayer.'->'.$toLayer;
        abort_unless(in_array($pair, ['bronze->silver','silver->gold']), 422);

        if (isset($v['transforms'])) $mapping->transforms = $v['transforms'];
        if (isset($v['dq_rules']))   $mapping->dq_rules   = $v['dq_rules'];

        $mapping->save();

        return redirect()->route('projects.show', $project)->with('success', 'Mapping mis à jour');
    }
}

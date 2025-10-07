<?php

namespace App\Http\Controllers;

use App\Models\Dataset;
use App\Models\Mapping;
use App\Models\Pipeline;
use App\Models\Project;
use Illuminate\Http\Request;
use Illuminate\Validation\Rule;

class PipelineController extends Controller
{
    public function store(Project $project, Request $r)
    {
        $v = $r->validate([
            'name' => [
                'required', 'string',
                Rule::unique('pipelines', 'name')->where('project_id', $project->id),
            ],
            'mapping_silver_id' => ['nullable', 'integer', 'exists:mappings,id'],
            'mapping_gold_id'   => ['nullable', 'integer', 'exists:mappings,id'],
        ]);

        [$silverOk, $goldOk] = $this->validateMappingsLayers($project, $v['mapping_silver_id'] ?? null, $v['mapping_gold_id'] ?? null);

        if (!$silverOk) return back()->withErrors(['mapping_silver_id' => 'Mapping must be Bronze → Silver on this project.']);
        if (!$goldOk)   return back()->withErrors(['mapping_gold_id'   => 'Mapping must be Silver → Gold on this project.']);

        $p = $project->pipelines()->create([
            'name'              => $v['name'],
            'mapping_silver_id' => $v['mapping_silver_id'] ?? null,
            'mapping_gold_id'   => $v['mapping_gold_id'] ?? null,
        ]);

        return back()->with('success', 'Pipeline created')->with('created_pipeline_id', $p->id);
    }

    public function update(Project $project, string $pipeline_id, Request $r)
    {
        /** @var Pipeline $pipe */
        $pipe = $project->pipelines()->findOrFail($pipeline_id);

        $v = $r->validate([
            'name' => [
                'sometimes', 'string',
                Rule::unique('pipelines', 'name')->where('project_id', $project->id)->ignore($pipe->id),
            ],
            'mapping_silver_id' => ['nullable', 'integer', 'exists:mappings,id'],
            'mapping_gold_id'   => ['nullable', 'integer', 'exists:mappings,id'],
        ]);

        [$silverOk, $goldOk] = $this->validateMappingsLayers(
            $project,
            $v['mapping_silver_id'] ?? $pipe->mapping_silver_id,
            $v['mapping_gold_id']   ?? $pipe->mapping_gold_id,
        );

        if (!$silverOk) return back()->withErrors(['mapping_silver_id' => 'Mapping must be Bronze → Silver on this project.']);
        if (!$goldOk)   return back()->withErrors(['mapping_gold_id'   => 'Mapping must be Silver → Gold on this project.']);

        $pipe->update($v);

        return back()->with('success', 'Pipeline updated');
    }

    public function destroy(Project $project, string $pipeline_id)
    {
        /** @var Pipeline $pipe */
        $pipe = $project->pipelines()->findOrFail($pipeline_id);
        $pipe->delete();

        return back()->with('success', 'Pipeline deleted');
    }

    /**
     * Ensure mapping_silver_id is Bronze->Silver and mapping_gold_id is Silver->Gold for THIS project
     * Both can be null.
     */
    private function validateMappingsLayers(Project $project, ?int $silverId, ?int $goldId): array
    {
        $silverOk = true;
        $goldOk = true;

        if ($silverId) {
            /** @var Mapping $m */
            $m = Mapping::where('project_id', $project->id)->find($silverId);
            if (!$m) $silverOk = false;
            else {
                /** @var Dataset $from */ $from = Dataset::find($m->from_dataset_id);
                /** @var Dataset $to   */ $to   = Dataset::find($m->to_dataset_id);
                $silverOk = $from && $to
                    && $from->project_id === $project->id && $to->project_id === $project->id
                    && $from->layer === 'bronze' && $to->layer === 'silver';
            }
        }

        if ($goldId) {
            /** @var Mapping $m */
            $m = Mapping::where('project_id', $project->id)->find($goldId);
            if (!$m) $goldOk = false;
            else {
                /** @var Dataset $from */ $from = Dataset::find($m->from_dataset_id);
                /** @var Dataset $to   */ $to   = Dataset::find($m->to_dataset_id);
                $goldOk = $from && $to
                    && $from->project_id === $project->id && $to->project_id === $project->id
                    && $from->layer === 'silver' && $to->layer === 'gold';
            }
        }

        return [$silverOk, $goldOk];
    }
}

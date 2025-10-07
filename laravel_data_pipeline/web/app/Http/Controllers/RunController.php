<?php

namespace App\Http\Controllers;

use App\Models\Project;
use App\Services\WorkerClient;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Throwable;

class RunController extends Controller
{
    public function store(Project $project, Request $r)
    {
        $v = $r->validate([
            'pipeline_id' => 'required|integer|exists:pipelines,id',
        ]);

        $pipeline = $project->pipelines()->with(['mapSilver.from_dataset.source','mapSilver.to_dataset','mapGold.from_dataset','mapGold.to_dataset'])->findOrFail($v['pipeline_id']);

        $run = $project->runs()->create([
            'pipeline_id' => $pipeline->id,
            'state'       => 'running',
            'started_at'  => now(),
        ]);

        try {
            $worker = new WorkerClient();

            $rows_source = $rows_source_rejected = $rows_silver = $rows_gold = null;
            $dq_summary = [];
            $logs = [];
            $silver_snapshot = $gold_snapshot = $bronze_snapshot = null;

            // ---- Étape 1 : Bronze -> Silver (si mapping_silver)
            if ($pipeline->mapping_silver_id) {
                $m = $pipeline->mapSilver;
                $from = $m->from_dataset;   // bronze
                $to   = $m->to_dataset;     // silver
                $src  = $from->source;

                // Payload DataSet
                $payload = [
                    'project'  => [
                        'namespace'     => $project->namespace,
                        'warehouse_uri' => $project->warehouse_uri,
                    ],
                    'datasets' => [
                        [
                            'Bronze' => [
                                'uri'    => $src->uri,
                                'source' => ['Csv' => $src->config],
                                'inner'  => [
                                    'name'        => $from->name,
                                    'primary_key' => [], // bronze
                                    'schema'      => $from->schema,
                                ],
                            ],
                        ],
                        [
                            'Silver' => [
                                'name'        => $to->name,         // "silver"
                                'primary_key' => $to->primary_key,  // requis
                                'schema'      => $to->schema,
                            ],
                        ],
                    ],
                    'mapping' => [
                        'transforms' => $m->transforms,
                        'dq_rules'   => $m->dq_rules ?? [],
                    ],
                ];

                $resp = $worker->run($payload);
                $logs = array_merge($logs, $resp['logs'] ?? []);

                // métriques
                $rows_source = $resp['ori_rows'] ?? null;
                $rows_silver = $resp['dest_rows'] ?? null;
                $rows_source_rejected = $resp['rejected_rows'] ?? null;
                $silver_snapshot = $resp['snapshot'] ?? null;
                $dq_summary = array_merge($dq_summary, $resp['dq_summary'] ?? []);

                // erreurs (échantillons)
                foreach (array_slice($resp['error_samples'] ?? [], 0, 1000) as $s) {
                    $run->errors()->create([
                        'project_id'   => $project->id,
                        'reason_code'  => $s['reason_code'] ?? 'ERR',
                        'message'      => $s['message'] ?? '',
                        'row_no'       => $s['row_no'] ?? null,
                        'source_values'=> $s['source_values'] ?? [],
                    ]);
                }
            }

            // ---- Étape 2 : Silver -> Gold (si mapping_gold)
            if ($pipeline->mapping_gold_id) {
                $m  = $pipeline->mapGold;
                $from = $m->from_dataset; // silver
                $to   = $m->to_dataset;   // gold

                $payload = [
                    'project'  => [
                        'namespace'     => $project->namespace,
                        'warehouse_uri' => $project->warehouse_uri,
                    ],
                    'datasets' => [
                        [
                            'Silver' => [
                                'name'        => $from->name,
                                'primary_key' => $from->primary_key,
                                'schema'      => $from->schema,
                            ],
                        ],
                        [
                            'Gold' => [
                                'name'        => $to->name,
                                'primary_key' => $to->primary_key,
                                'schema'      => $to->schema,
                            ],
                        ],
                    ],
                    'mapping' => [
                        'transforms' => $m->transforms,
                        'dq_rules'   => $m->dq_rules ?? [],
                    ],
                ];

                $resp = $worker->run($payload);
                $logs = array_merge($logs, $resp['logs'] ?? []);

                $rows_gold     = $resp['dest_rows'] ?? null;
                $gold_snapshot = $resp['snapshot'] ?? null;
                $dq_summary = array_merge($dq_summary, $resp['dq_summary'] ?? []);

                foreach (array_slice($resp['error_samples'] ?? [], 0, 1000) as $s) {
                    $run->errors()->create([
                        'project_id'   => $project->id,
                        'reason_code'  => $s['reason_code'] ?? 'ERR',
                        'message'      => $s['message'] ?? '',
                        'row_no'       => $s['row_no'] ?? null,
                        'source_values'=> $s['source_values'] ?? [],
                    ]);
                }
            }

            DB::transaction(function () use ($run, $bronze_snapshot, $silver_snapshot, $gold_snapshot, $rows_source, $rows_source_rejected, $rows_silver, $rows_gold, $dq_summary, $logs) {
                $run->update([
                    'state'                 => 'succeeded',
                    'bronze_snapshot'       => $bronze_snapshot,
                    'silver_snapshot'       => $silver_snapshot,
                    'gold_snapshot'         => $gold_snapshot,
                    'rows_source'           => $rows_source,
                    'rows_source_rejected'  => $rows_source_rejected,
                    'rows_silver'           => $rows_silver,
                    'rows_silver_rejected'  => null,
                    'rows_gold'             => $rows_gold,
                    'dq_summary'            => $dq_summary,
                    'logs'                  => $logs,
                    'finished_at'           => now(),
                ]);
            });
        } catch (Throwable $e) {
            $run->update([
                'state' => 'failed',
                'state_reason' => $e->getMessage(),
                'finished_at' => now(),
            ]);
        }

        return back();
    }
}

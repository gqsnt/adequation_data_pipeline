<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class PipelineRun extends Model
{

    protected $fillable = [
        'project_id','pipeline_id','state','state_reason',
        'bronze_snapshot','silver_snapshot','gold_snapshot',
        'rows_source','rows_source_rejected','rows_silver','rows_silver_rejected','rows_gold',
        'started_at','finished_at','dq_summary','logs'
    ];

    protected $casts = [
        'dq_summary' => 'array',
        'logs' => 'array',
        'started_at' => 'datetime',
        'finished_at' => 'datetime',
    ];

    public function project(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(Project::class);
    }



    public function pipeline(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(Pipeline::class);
    }

    public function errors(): \Illuminate\Database\Eloquent\Relations\HasMany
    {
        return $this->hasMany(RunErrorSample::class, 'run_id');
    }
}

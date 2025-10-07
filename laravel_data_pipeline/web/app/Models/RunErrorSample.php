<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class RunErrorSample extends Model
{

    protected $casts = ['source_values' => 'array'];
    protected $fillable = [
        "project_id",
        "run_id",
        "reason_code",
        "message",
        "row_no",
        "source_values",
    ];

    public function run(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(PipelineRun::class, 'run_id');
    }

    public function project(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(Project::class);
    }

}

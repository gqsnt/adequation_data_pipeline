<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Pipeline extends Model
{
    protected $fillable = [
            'project_id','name',
          'mapping_silver_id',
            'mapping_gold_id'
        ];

    public function project(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    { return $this->belongsTo(Project::class); }

    public function mapSilver(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    { return $this->belongsTo(Mapping::class,'mapping_silver_id'); }
    public function mapGold(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    { return $this->belongsTo(Mapping::class,'mapping_gold_id'); }
    public function runs() { return $this->hasMany(PipelineRun::class); }
}

<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Dataset extends Model
{
    protected $fillable = [
        "project_id",
        "source_id",
        "layer",
        "name",
        "schema",
        "primary_key"
    ];

    protected $casts = ['schema'=>'array', 'primary_key'=>'array'];



    public function project(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    { return $this->belongsTo(Project::class); }
    public function source(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    { return $this->belongsTo(Source::class); }
}

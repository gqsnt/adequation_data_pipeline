<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasOne;

class Source extends Model
{
    protected $fillable = [
        'project_id',
        "name",
        "uri",
        "config",
    ];

    protected $casts = ['config' => 'array'];


    public function project(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(Project::class);
    }

    public function bronze(): HasOne
    {
        return $this->hasOne(Dataset::class, 'source_id')->where('layer', 'bronze');
    }
}

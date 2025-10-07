<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Mapping extends Model
{

    protected $casts = ['transforms' => 'array', 'dq_rules' => 'array'];
    protected $fillable = ['project_id', 'from_dataset_id', 'to_dataset_id', 'transforms', 'dq_rules'];

    public function project(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(Project::class);
    }

    public function from_dataset(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(Dataset::class, 'from_dataset_id');
    }

    public function to_dataset(): \Illuminate\Database\Eloquent\Relations\BelongsTo
    {
        return $this->belongsTo(Dataset::class, 'to_dataset_id');
    }

}

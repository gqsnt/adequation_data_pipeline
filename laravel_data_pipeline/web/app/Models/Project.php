<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Concerns\HasUuids;
class Project extends Model
{
    use HasUuids;

    public $incrementing = false;
    protected $keyType = 'string';

    protected $fillable = [
        "id",
        'slug',
        'warehouse_uri',
        'namespace',
    ];


    public function datasets(): \Illuminate\Database\Eloquent\Relations\HasMany
    { return $this->hasMany(Dataset::class); }
    public function sources(): \Illuminate\Database\Eloquent\Relations\HasMany
    { return $this->hasMany(Source::class); }
    public function mappings(): \Illuminate\Database\Eloquent\Relations\HasMany
    { return $this->hasMany(Mapping::class); }
    public function pipelines(): \Illuminate\Database\Eloquent\Relations\HasMany
    { return $this->hasMany(Pipeline::class); }

    public function runs(): \Illuminate\Database\Eloquent\Relations\HasMany
    { return $this->hasMany(PipelineRun::class); }
}

<?php

use App\Http\Controllers\GoldController;
use App\Http\Controllers\MappingController;
use App\Http\Controllers\ProjectController;
use App\Http\Controllers\RunController;
use App\Http\Controllers\SilverController;
use App\Http\Controllers\SourceController;
use Illuminate\Support\Facades\Route;
use Inertia\Inertia;

Route::get('/', function () {
    return Inertia::render('Welcome');
})->name('home');

Route::get('dashboard', function () {
    return Inertia::render('Dashboard');
})->middleware(['auth', 'verified'])->name('dashboard');

Route::prefix('projects')->middleware(['auth', 'verified'])->group(function () {
    // Projects
    Route::get('/', [ProjectController::class, 'index'])->name('projects.index');
    Route::post('/', [ProjectController::class, 'store'])->name('projects.store');
    Route::get('/{project}', [ProjectController::class, 'show'])->name('projects.show');

    // Sources
    Route::post('/{project}/sources', [SourceController::class, 'store'])->name('sources.store');
    Route::put('/{project}/sources/{source}', [SourceController::class, 'update'])->name('sources.update');
    Route::delete('/{project}/sources/{source}', [SourceController::class, 'destroy'])->name('sources.destroy');
    Route::post('/{project}/sources/{source}/infer_schema', [SourceController::class, 'infer_schema'])->name('sources.preview');


    // Silver (schéma de projet unique)
    Route::get('/{project}/silver', [SilverController::class, 'show'])->name('silver.show');
    Route::put('/{project}/silver', [SilverController::class, 'update'])->name('silver.update');
    Route::post('/{project}/silver/seed-from-source/{source}', [SilverController::class, 'seedFromSource'])->name('silver.seed');

    //gold
    Route::post('/{project}/gold', [\App\Http\Controllers\GoldDatasetController::class,'store'])->name('gold.store');
    Route::put('/{project}/gold/{dataset}', [\App\Http\Controllers\GoldDatasetController::class,'update'])->name('gold.update');
    Route::delete('/{project}/gold/{dataset}', [\App\Http\Controllers\GoldDatasetController::class,'destroy'])->name('gold.destroy');



    // Mapping
    Route::post('/{project}/mappings', [MappingController::class, 'store'])->name('mappings.store');
    Route::put('/{project}/mappings/{mapping}', [MappingController::class, 'update'])->name('mappings.update');


    // Pipelines
    Route::post('/{project}/pipelines', [\App\Http\Controllers\PipelineController::class,'store'])->name('pipelines.store');
    Route::put('/{project}/pipelines/{pipeline_id}', [\App\Http\Controllers\PipelineController::class,'update'])->name('pipelines.update');
    Route::delete('/{project}/pipelines/{pipeline_id}', [\App\Http\Controllers\PipelineController::class,'destroy'])->name('pipelines.destroy');


    // Runs
    Route::post('/{project}/runs', [\App\Http\Controllers\RunController::class,'store'])->name('runs.store');

});

require __DIR__ . '/settings.php';
require __DIR__ . '/auth.php';

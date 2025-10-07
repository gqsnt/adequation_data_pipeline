<?php

use App\Http\Controllers\DvfController;
use Illuminate\Support\Facades\Route;


Route::get('/', function () {
    return view('welcome');
});

Route::get('/dvf/ingest', [DvfController::class, 'ingest']);       // triggers ORM-based ingestion
Route::get('/dvf/filter', [DvfController::class, 'filter']);       // range filter by dep/date
Route::get('/dvf/agg-commune', [DvfController::class, 'aggByCommune']); // aggregates with median/avg
Route::get('/dvf/topn', [DvfController::class, 'topN']);           // top-N communes by avg prix_m2
Route::get('/dvf/geo-bbox', [DvfController::class, 'geoBbox']);

<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {

        Schema::create('run_error_samples', function (Blueprint $table) {
            $table->id();
            $table->foreignUuid('project_id')->constrained("projects", "id")->onDelete('cascade');
            $table->foreignId("run_id")->constrained("pipeline_runs")->onDelete('cascade');
            $table->text('reason_code');
            $table->text('message');
            $table->bigInteger('row_no')->nullable();
            $table->json('source_values')->nullable();
            $table->timestamps();
            $table->index(['run_id'], 'idx_run_error_sample_run_id');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('run_error_samples');
    }
};

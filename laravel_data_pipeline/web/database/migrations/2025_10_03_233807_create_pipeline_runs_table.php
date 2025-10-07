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
        Schema::create('pipeline_runs', function (Blueprint $table) {
            $table->id();
            $table->foreignUuid('project_id')->constrained("projects", "id")->onDelete('cascade');
            $table->foreignId('pipeline_id')->constrained('pipelines')->onDelete('cascade');
            $table->enum('state', ['queued', 'running', 'succeeded', 'failed'])->default('queued');
            $table->text('state_reason')->nullable();
            $table->text('bronze_snapshot')->nullable();
            $table->text('silver_snapshot')->nullable();
            $table->text('gold_snapshot')->nullable();
            $table->bigInteger('rows_source')->nullable();
            $table->bigInteger('rows_source_rejected')->nullable();
            $table->bigInteger('rows_silver')->nullable();
            $table->bigInteger('rows_silver_rejected')->nullable();
            $table->bigInteger('rows_gold')->nullable();
            $table->timestamp('started_at')->nullable();
            $table->timestamp('finished_at')->nullable();
            $table->json('dq_summary')->nullable();
            $table->json('logs')->nullable();
            $table->timestamps();
            $table->index(['project_id', 'created_at'], 'idx_pipeline_run_project_created_at');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('pipeline_runs');
    }
};

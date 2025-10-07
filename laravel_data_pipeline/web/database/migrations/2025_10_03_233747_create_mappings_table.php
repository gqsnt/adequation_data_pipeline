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
        Schema::create('mappings', function (Blueprint $table) {
            $table->id();
            $table->foreignUuid('project_id')->constrained("projects", "id")->onDelete('cascade');
            $table->foreignId("from_dataset_id")->constrained("datasets")->onDelete('cascade');
            $table->foreignId("to_dataset_id")->constrained("datasets")->onDelete('cascade');
            $table->json("transforms");
            $table->json("dq_rules");
            $table->timestamps();
            $table->unique(['project_id','from_dataset_id','to_dataset_id'], 'unique_mapping_per_project_from_to');
            $table->index(['project_id','from_dataset_id','to_dataset_id'], 'idx_mapping_from_to');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('mappings');
    }
};

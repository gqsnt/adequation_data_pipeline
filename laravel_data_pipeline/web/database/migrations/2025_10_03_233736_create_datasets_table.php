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
        Schema::create('datasets', function (Blueprint $table) {
            $table->id();
            $table->foreignUuid('project_id')->constrained("projects", "id")->onDelete('cascade');
            $table->foreignId('source_id')->nullable()->constrained("sources", "id")->onDelete('cascade');
            $table->string("name");
            $table->enum("layer", ["bronze", "silver", "gold"]);
            $table->json("schema");
            $table->json("primary_key")->nullable();
            $table->timestamps();
            $table->unique(['project_id','layer', 'name'], 'unique_dataset_per_project_layer_name');
        });

    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('datasets');
    }
};

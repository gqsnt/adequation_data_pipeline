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
        Schema::create('sources', function (Blueprint $table) {
            $table->id();
            $table->foreignUuid('project_id')->constrained("projects", "id")->onDelete('cascade');
            $table->string("name");
            $table->string("uri");
            $table->json("config");
            $table->timestamps();
            $table->unique(['project_id','name'], 'unique_source_per_project_name');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('sources');
    }
};

<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('dvfs', function (Blueprint $table) {
            $table->id();

            $table->string('id_mutation', 64)->nullable()->index();
            $table->date('date_mutation')->nullable()->index();
            $table->smallInteger('year')->nullable()->index();
            $table->bigInteger('numero_disposition')->nullable();

            $table->string('nature_mutation', 64)->nullable()->index();
            $table->double('valeur_fonciere')->nullable();

            $table->bigInteger('adresse_numero')->nullable();
            $table->string('adresse_suffixe', 32)->nullable();
            $table->string('adresse_nom_voie', 256)->nullable()->index();
            $table->string('adresse_code_voie', 32)->nullable();

            $table->bigInteger('code_postal')->nullable()->index();
            $table->bigInteger('code_commune')->nullable()->index();
            $table->string('nom_commune', 128)->nullable()->index();
            $table->bigInteger('code_departement')->nullable()->index();

            $table->string('ancien_code_commune', 32)->nullable();
            $table->string('ancien_nom_commune', 128)->nullable();

            $table->string('id_parcelle', 64)->nullable()->index();
            $table->string('ancien_id_parcelle', 64)->nullable();

            $table->string('numero_volume', 32)->nullable();

            $table->bigInteger('lot1_numero')->nullable();
            $table->double('lot1_surface_carrez')->nullable();
            $table->bigInteger('lot2_numero')->nullable();
            $table->double('lot2_surface_carrez')->nullable();
            $table->bigInteger('lot3_numero')->nullable();
            $table->double('lot3_surface_carrez')->nullable();
            $table->bigInteger('lot4_numero')->nullable();
            $table->double('lot4_surface_carrez')->nullable();
            $table->bigInteger('lot5_numero')->nullable();
            $table->double('lot5_surface_carrez')->nullable();

            $table->bigInteger('nombre_lots')->nullable();
            $table->bigInteger('code_type_local')->nullable()->index();
            $table->string('type_local', 64)->nullable()->index();

            $table->bigInteger('surface_reelle_bati')->nullable()->index();
            $table->bigInteger('nombre_pieces_principales')->nullable();

            $table->string('code_nature_culture', 32)->nullable();
            $table->string('nature_culture', 64)->nullable();
            $table->string('code_nature_culture_speciale', 32)->nullable();
            $table->string('nature_culture_speciale', 64)->nullable();

            $table->bigInteger('surface_terrain')->nullable();

            $table->double('longitude')->nullable();
            $table->double('latitude')->nullable();

            // Derived
            $table->double('prix_m2')->nullable()->index();

            // Composite indexes for common predicates
            $table->index(['code_departement', 'year', 'date_mutation'], 'dvf_dep_year_date_idx');
            $table->index(['longitude', 'latitude'], 'dvf_lon_lat_idx');
            // No timestamps for this analytical table
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('dvfs');
    }
};

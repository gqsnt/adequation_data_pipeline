<?php

namespace App\Http\Controllers;

use App\Models\Dvf;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use SplFileObject;

class DvfController extends Controller
{
    private const CSV_PATH = '../samples/dvf.csv';
    private const BATCH_SIZE = 200;

    // GET /api/dvf/ingest?n=100000   (0 or missing => unlimited)
    public function ingest(Request $request)
    {
        @set_time_limit(0);
        @ini_set('memory_limit', '-1');

        DB::disableQueryLog();

        $n = (int)$request->query('n', 0);
        $limit = $n > 0 ? $n : PHP_INT_MAX;

        $path = base_path(self::CSV_PATH);
        if (!is_readable($path)) {
            return response()->json(['ok' => false, 'error' => "CSV not readable at $path"], 400);
        }

        $t0 = microtime(true);
        $file = new SplFileObject($path, 'r');
        $file->setFlags(
            SplFileObject::READ_CSV |
            SplFileObject::READ_AHEAD |
            SplFileObject::DROP_NEW_LINE |
            SplFileObject::SKIP_EMPTY
        );
        $file->setCsvControl(',');

        // Header
        $header = $file->fgetcsv();
        if (!$header || count($header) < 10) {
            return response()->json(['ok' => false, 'error' => 'Invalid header'], 400);
        }
        $idx = [];
        foreach ($header as $i => $name) {
            if ($name !== null && $name !== '') $idx[trim($name)] = $i;
        }

        $rows = 0;
        $inserted = 0;
        $rejected = 0;
        $batch = [];

        while (!$file->eof() && $rows < $limit) {
            $row = $file->fgetcsv();
            if ($row === false || $row === [null]) {
                continue;
            }
            $rows++;

            $rec = $this->mapRow($idx, $row);
            if ($rec === null) {
                $rejected++;
                continue;
            }

            $batch[] = $rec;

            if (count($batch) >= self::BATCH_SIZE) {
                DB::transaction(function () use (&$batch, &$inserted) {
                    Dvf::withoutEvents(function () use (&$batch, &$inserted) {
                        Dvf::insert($batch);
                        $inserted += count($batch);
                        $batch = [];
                    });
                });
            }
        }

        if (!empty($batch)) {
            DB::transaction(function () use (&$batch, &$inserted) {
                Dvf::withoutEvents(function () use (&$batch, &$inserted) {
                    Dvf::insert($batch);
                    $inserted += count($batch);
                    $batch = [];
                });
            });
        }

        $sec = microtime(true) - $t0;
        return response()->json([
            'ok' => true,
            'requested_rows' => $n,
            'processed_rows' => $rows,
            'inserted' => $inserted,
            'rejected' => $rejected,
            'seconds' => round($sec, 3),
            'rate_rows_per_sec' => $sec > 0 ? round($inserted / $sec, 2) : null
        ]);
    }

    // GET /api/dvf/filter?dep=75&from=2023-01-01&to=2023-12-31&page=1&per_page=50
    public function filter(Request $request)
    {
        $dep = (int)$request->query('dep');
        $from = $request->query('from');
        $to = $request->query('to');

        $q = Dvf::query()->where('code_departement', $dep);
        if ($from) $q->where('date_mutation', '>=', $from);
        if ($to) $q->where('date_mutation', '<=', $to);

        $perPage = min((int)$request->query('per_page', 50), 500);
        $res = $q->orderBy('date_mutation', 'desc')->paginate($perPage);

        return response()->json($res);
    }

    // GET /api/dvf/agg-commune?dep=75&year=2023
    public function aggByCommune(Request $request)
    {
        $dep = (int)$request->query('dep');
        $year = (int)$request->query('year');

        $rows = Dvf::query()
            ->selectRaw('nom_commune,
                COUNT(*) AS n,
                AVG(prix_m2) AS avg_prix_m2,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY prix_m2) AS p50_prix_m2')
            ->where('code_departement', $dep)
            ->where('year', $year)
            ->whereNotNull('prix_m2')
            ->groupBy('nom_commune')
            ->orderByDesc('avg_prix_m2')
            ->limit(100)
            ->get();

        return response()->json($rows);
    }

    // GET /api/dvf/topn?dep=75&year=2023&n=20
    public function topN(Request $request)
    {
        $dep = (int)$request->query('dep');
        $year = (int)$request->query('year');
        $n = min((int)$request->query('n', 20), 200);

        $rows = Dvf::query()
            ->selectRaw('nom_commune,
                AVG(prix_m2) AS avg_prix_m2,
                COUNT(*) AS n')
            ->where('code_departement', $dep)
            ->where('year', $year)
            ->whereNotNull('prix_m2')
            ->groupBy('nom_commune')
            ->orderByDesc('avg_prix_m2')
            ->limit($n)
            ->get();

        return response()->json($rows);
    }

    // GET /api/dvf/geo-bbox?min_lon=2.1&min_lat=48.7&max_lon=2.5&max_lat=48.95&year=2023
    public function geoBbox(Request $request)
    {
        $minLon = (float)$request->query('min_lon');
        $minLat = (float)$request->query('min_lat');
        $maxLon = (float)$request->query('max_lon');
        $maxLat = (float)$request->query('max_lat');
        $year = $request->query('year');

        $q = Dvf::query()
            ->whereBetween('longitude', [$minLon, $maxLon])
            ->whereBetween('latitude', [$minLat, $maxLat]);

        if ($year) $q->where('year', (int)$year);

        $perPage = min((int)$request->query('per_page', 50), 500);
        $res = $q->orderBy('date_mutation', 'desc')->paginate($perPage);

        return response()->json($res);
    }

    // ---- Hot path helpers (no Carbon) ----

    private function mapRow(array $idx, array $row): ?array
    {
        $g = fn(string $k) => array_key_exists($k, $idx) ? ($row[$idx[$k]] ?? null) : null;

        $date = $this->parseDateStr($g('date_mutation'));
        if ($date === null) return null;
        $year = (int)substr($date, 0, 4);

        $valeur = $this->toFloat($g('valeur_fonciere'));
        $bati = $this->toInt($g('surface_reelle_bati'));
        if ($valeur !== null && $valeur <= 0) return null;
        if ($bati !== null && $bati < 0) return null;

        $prix_m2 = ($bati && $bati > 0 && $valeur !== null) ? ($valeur / $bati) : null;

        $lon = $this->toFloat($g('longitude'));
        $lat = $this->toFloat($g('latitude'));
        if ($lon !== null && ($lon < -180 || $lon > 180)) $lon = null;
        if ($lat !== null && ($lat < -90 || $lat > 90)) $lat = null;

        return [
            'id_mutation' => $this->normStr($g('id_mutation')),
            'date_mutation' => $date,
            'year' => $year,
            'numero_disposition' => $this->toInt($g('numero_disposition')),
            'nature_mutation' => $this->normStr($g('nature_mutation')),
            'valeur_fonciere' => $valeur,

            'adresse_numero' => $this->toInt($g('adresse_numero')),
            'adresse_suffixe' => $this->normStr($g('adresse_suffixe')),
            'adresse_nom_voie' => $this->normStr($g('adresse_nom_voie')),
            'adresse_code_voie' => $this->normStr($g('adresse_code_voie')),

            'code_postal' => $this->toInt($g('code_postal')),
            'code_commune' => $this->toInt($g('code_commune')),
            'nom_commune' => $this->upper($g('nom_commune')),
            'code_departement' => $this->toInt($g('code_departement')),

            'ancien_code_commune' => $this->normStr($g('ancien_code_commune')),
            'ancien_nom_commune' => $this->normStr($g('ancien_nom_commune')),

            'id_parcelle' => $this->normStr($g('id_parcelle')),
            'ancien_id_parcelle' => $this->normStr($g('ancien_id_parcelle')),
            'numero_volume' => $this->normStr($g('numero_volume')),

            'lot1_numero' => $this->toInt($g('lot1_numero')),
            'lot1_surface_carrez' => $this->toFloat($g('lot1_surface_carrez')),
            'lot2_numero' => $this->toInt($g('lot2_numero')),
            'lot2_surface_carrez' => $this->toFloat($g('lot2_surface_carrez')),
            'lot3_numero' => $this->toInt($g('lot3_numero')),
            'lot3_surface_carrez' => $this->toFloat($g('lot3_surface_carrez')),
            'lot4_numero' => $this->toInt($g('lot4_numero')),
            'lot4_surface_carrez' => $this->toFloat($g('lot4_surface_carrez')),
            'lot5_numero' => $this->toInt($g('lot5_numero')),
            'lot5_surface_carrez' => $this->toFloat($g('lot5_surface_carrez')),

            'nombre_lots' => $this->toInt($g('nombre_lots')),
            'code_type_local' => $this->toInt($g('code_type_local')),
            'type_local' => $this->title($g('type_local')),
            'surface_reelle_bati' => $bati,
            'nombre_pieces_principales' => $this->toInt($g('nombre_pieces_principales')),

            'code_nature_culture' => $this->normStr($g('code_nature_culture')),
            'nature_culture' => $this->normStr($g('nature_culture')),
            'code_nature_culture_speciale' => $this->normStr($g('code_nature_culture_speciale')),
            'nature_culture_speciale' => $this->normStr($g('nature_culture_speciale')),

            'surface_terrain' => $this->toInt($g('surface_terrain')),
            'longitude' => $lon,
            'latitude' => $lat,
            'prix_m2' => $prix_m2,
        ];
    }

    private function parseDateStr($raw): ?string
    {
        $s = $this->normStr($raw);
        if ($s === null) return null;
        $s = str_replace('/', '-', $s);
        // Fast-path: YYYY-MM-DD
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $s)) return $s;
        // Fallback: DD-MM-YYYY
        if (preg_match('/^(\d{2})-(\d{2})-(\d{4})$/', $s, $m)) {
            return "{$m[3]}-{$m[2]}-{$m[1]}";
        }
        return null;
    }

    private function normStr($v): ?string
    {
        if ($v === null) return null;
        $s = trim((string)$v);
        if ($s === '' || in_array(strtolower($s), ['na', 'n/a', 'null'], true)) return null;
        return preg_replace('/\s+/u', ' ', $s);
    }

    private function upper($v): ?string
    {
        $s = $this->normStr($v);
        return $s ? mb_strtoupper($s, 'UTF-8') : null;
    }

    private function title($v): ?string
    {
        $s = $this->normStr($v);
        return $s ? mb_convert_case(mb_strtolower($s, 'UTF-8'), MB_CASE_TITLE_SIMPLE, 'UTF-8') : null;
    }

    private function toInt($v): ?int
    {
        $s = $this->normStr($v);
        if ($s === null || !is_numeric($s)) return null;
        return (int)$s;
    }

    private function toFloat($v): ?float
    {
        $s = $this->normStr($v);
        if ($s === null) return null;
        $s = str_replace(',', '.', $s);
        return is_numeric($s) ? (float)$s : null;
    }
}

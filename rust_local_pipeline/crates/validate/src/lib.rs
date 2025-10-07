use anyhow::{anyhow, Context, Result};
 use arrow::array::{
         Array, ArrayRef, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
         Float64Builder, Int16Builder, Int32Array, Int32Builder, Int64Array, StringArray, StringBuilder,
     };
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader as IpcReader;
use arrow::ipc::writer::FileWriter as IpcWriter;
use arrow::record_batch::RecordBatch;
use blake3::Hasher;
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::sync::Arc;
use time::{format_description, Date, Month};

#[derive(Debug, Clone)]
pub struct ValidateConfig {
    pub slug: String,          // e.g., "dvf"
    pub ingest_date: String,   // "YYYY-MM-DD"
    pub storage_root: PathBuf, // "./data"
    pub bronze_dir: String,    // "bronze"
    pub silver_dir: String,    // "silver"
    pub rejects_dir: String,   // "rejects"
}

#[derive(Debug, Default, Clone)]
pub struct ValidationStats {
    pub rows_in: u64,
    pub rows_out: u64,
    pub rejects: u64,
    pub silver_out: PathBuf,
    pub rejects_out: PathBuf,
}

pub async fn validate_dataset(cfg: ValidateConfig) -> Result<ValidationStats> {
    // Locate Bronze file
    let bronze_dir = cfg
        .storage_root
        .join(&cfg.bronze_dir)
        .join(&cfg.slug)
        .join(format!("ingest_date={}", cfg.ingest_date));
    let bronze_path = bronze_dir.join("part-000000.arrow");
    if !bronze_path.exists() {
        return Err(anyhow!("Bronze file not found: {}", bronze_path.display()));
    }

    // Prepare output dirs
    let silver_dir = cfg
        .storage_root
        .join(&cfg.silver_dir)
        .join(&cfg.slug)
        .join(format!("ingest_date={}", cfg.ingest_date));
    create_dir_all(&silver_dir)?;
    let silver_out = silver_dir.join("part-000000.arrow");

    let rejects_dir = cfg
        .storage_root
        .join(&cfg.rejects_dir)
        .join(&cfg.slug)
        .join(format!("ingest_date={}", cfg.ingest_date));
    create_dir_all(&rejects_dir)?;
    let rejects_out = rejects_dir.join("part-000000.arrow");

    // Open Bronze IPC
    let f = File::open(&bronze_path).with_context(|| format!("open {}", bronze_path.display()))?;
    let reader = IpcReader::try_new(f, None)?;
    let bronze_schema = reader.schema();

    // Column indices (all Utf8 except lineage row_number:Int64)
    let idx = BronzeIdx::from_schema(&bronze_schema)?;

    // Silver schema (typed)
    let silver_schema = Arc::new(silver_schema());

    // Writers
    let mut silver_writer = IpcWriter::try_new(File::create(&silver_out)?, &silver_schema)?;
    let rejects_schema = Arc::new(rejects_schema(&bronze_schema)?);
    let mut rejects_writer = IpcWriter::try_new(File::create(&rejects_out)?, &rejects_schema)?;

    // Dedup tracking
    let mut seen_keys: HashSet<[u8; 32]> = HashSet::new();

    // Builders (batching)
    let mut b = SilverBuilders::try_new(BATCH_SIZE)?;
     let mut r = RejectBuilders::new(
         rejects_schema.clone(),
         bronze_schema.fields().len(),
         BATCH_SIZE,
     )?;

    let mut rows_in: u64 = 0;
    let mut rows_out: u64 = 0;
    let mut rejects: u64 = 0;
    let mut batch_rows: usize = 0;

    // Date parser
    let fmt = format_description::parse("[year]-[month]-[day]")?;

    for maybe_batch in reader {
        let batch = maybe_batch?;
        let n = batch.num_rows();
        rows_in += n as u64;

        // convenient accessors
        let get_s = |name: &str| -> Result<&StringArray> {
            Ok(batch
                .column(idx.get(name)?)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected Utf8 for column {}", name))?)
        };

        // Required columns
        let id_mutation = get_s("id_mutation")?;
        let date_mutation_s = get_s("date_mutation")?;
        let numero_disposition = get_s("numero_disposition")?;
        let nature_mutation = get_s("nature_mutation")?;

        // Optionals (Utf8)
        let valeur_fonciere = as_utf8_opt(&batch, idx.idx_valeur_fonciere);
        let adresse_numero = as_utf8_opt(&batch, idx.idx_adresse_numero);
        let adresse_suffixe = as_utf8_opt(&batch, idx.idx_adresse_suffixe);
        let adresse_nom_voie = as_utf8_opt(&batch, idx.idx_adresse_nom_voie);
        let adresse_code_voie = as_utf8_opt(&batch, idx.idx_adresse_code_voie);
        let code_postal = as_utf8_opt(&batch, idx.idx_code_postal);
        let code_commune = as_utf8_opt(&batch, idx.idx_code_commune);
        let nom_commune = as_utf8_opt(&batch, idx.idx_nom_commune);
        let code_departement = as_utf8_opt(&batch, idx.idx_code_departement);
        let ancien_code_commune = as_utf8_opt(&batch, idx.idx_ancien_code_commune);
        let ancien_nom_commune = as_utf8_opt(&batch, idx.idx_ancien_nom_commune);
        let id_parcelle = as_utf8_opt(&batch, idx.idx_id_parcelle);
        let ancien_id_parcelle = as_utf8_opt(&batch, idx.idx_ancien_id_parcelle);
        let numero_volume = as_utf8_opt(&batch, idx.idx_numero_volume);

        let lot1_numero = as_utf8_opt(&batch, idx.idx_lot1_numero);
        let lot1_surface_carrez = as_utf8_opt(&batch, idx.idx_lot1_surface_carrez);
        let lot2_numero = as_utf8_opt(&batch, idx.idx_lot2_numero);
        let lot2_surface_carrez = as_utf8_opt(&batch, idx.idx_lot2_surface_carrez);
        let lot3_numero = as_utf8_opt(&batch, idx.idx_lot3_numero);
        let lot3_surface_carrez = as_utf8_opt(&batch, idx.idx_lot3_surface_carrez);
        let lot4_numero = as_utf8_opt(&batch, idx.idx_lot4_numero);
        let lot4_surface_carrez = as_utf8_opt(&batch, idx.idx_lot4_surface_carrez);
        let lot5_numero = as_utf8_opt(&batch, idx.idx_lot5_numero);
        let lot5_surface_carrez = as_utf8_opt(&batch, idx.idx_lot5_surface_carrez);

        let nombre_lots = as_utf8_opt(&batch, idx.idx_nombre_lots);
        let code_type_local = as_utf8_opt(&batch, idx.idx_code_type_local);
        let type_local = as_utf8_opt(&batch, idx.idx_type_local);
        let surface_reelle_bati = as_utf8_opt(&batch, idx.idx_surface_reelle_bati);
        let nombre_pieces_principales = as_utf8_opt(&batch, idx.idx_nombre_pieces_principales);
        let code_nature_culture = as_utf8_opt(&batch, idx.idx_code_nature_culture);
        let nature_culture = as_utf8_opt(&batch, idx.idx_nature_culture);
        let code_nature_culture_speciale =
            as_utf8_opt(&batch, idx.idx_code_nature_culture_speciale);
        let nature_culture_speciale = as_utf8_opt(&batch, idx.idx_nature_culture_speciale);
        let surface_terrain = as_utf8_opt(&batch, idx.idx_surface_terrain);
        let longitude_s = as_utf8_opt(&batch, idx.idx_longitude);
        let latitude_s = as_utf8_opt(&batch, idx.idx_latitude);


        // Lineage
        let _ingest_date = get_s("ingest_date")?;
        let _source_file = get_s("source_file")?;
        let _row_number = batch
            .column(idx.idx_row_number)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("expected Int64 for row_number"))?;

        for row in 0..n {
            // Required presence
            if id_mutation.is_null(row)
                || date_mutation_s.is_null(row)
                || numero_disposition.is_null(row)
            {
                r.push_reject(&batch, row, "DVF_SCHEMA_MISSING", "missing required column")?;
                rejects += 1;
                continue;
            }

            let id_mut = id_mutation.value(row).trim();
            let num_disp = numero_disposition.value(row).trim();

            // Parse date -> Date32 + year bounds
            let date_s = date_mutation_s.value(row).trim();
            let date_parsed: Date = match Date::parse(date_s, &fmt) {
                Ok(d) => d,
                Err(_) => {
                    r.push_reject(&batch, row, "DVF_DATE_INVALID", "date parse failed")?;
                    rejects += 1;
                    continue;
                }
            };
            let y = date_parsed.year();
            if !(1990..=date_parsed.year() + 1).contains(&y) {
                r.push_reject(
                    &batch,
                    row,
                    "DVF_DATE_INVALID",
                    "year out of expected bounds",
                )?;
                rejects += 1;
                continue;
            }
            let date_days = date32_from_date(date_parsed);

            // valeur_fonciere -> Decimal128(12,2)
            let (has_valeur, valeur_scaled) = match opt_str(valeur_fonciere, row) {
                None => (false, 0i128),
                Some(s) => match parse_decimal_2(s) {
                    Some(v) if v >= 0 => (true, v),
                    _ => {
                        r.push_reject(
                            &batch,
                            row,
                            "DVF_VALUE_NEGATIVE",
                            "valeur_fonciere invalid/negative",
                        )?;
                        rejects += 1;
                        continue;
                    }
                },
            };

            let prix_m2_val = match (has_valeur, opt_str(surface_reelle_bati, row).and_then(|s| s.parse::<i64>().ok())) {
                (true, Some(surf)) if surf > 9 => {
                    let v = valeur_scaled as f64 / 100.0; // cents -> €
                    Some(v / (surf as f64))
                }
                _ => None
            };



            // lon/lat pair
            let lon_opt = opt_str(longitude_s, row).and_then(parse_f64);
            let lat_opt = opt_str(latitude_s, row).and_then(parse_f64);
            if (lon_opt.is_some() && lat_opt.is_none()) || (lon_opt.is_none() && lat_opt.is_some())
            {
                r.push_reject(
                    &batch,
                    row,
                    "DVF_COORD_OOB",
                    "lon/lat must be both present or both null",
                )?;
                rejects += 1;
                continue;
            }
            if let (Some(lon), Some(lat)) = (lon_opt, lat_opt) {
                if lon < -5.5 || lon > 9.9 || lat < 41.0 || lat > 51.5 {
                    r.push_reject(&batch, row, "DVF_COORD_OOB", "lon/lat out of FR bbox")?;
                    rejects += 1;
                    continue;
                }
            }



            // Normalize uppercase for a few text fields
            let nat_mut = nature_mutation.value(row).trim().to_uppercase();
            let nom_com = opt_str(nom_commune, row).map(|s| s.trim().to_uppercase());
            let typ_loc = opt_str(type_local, row).map(|s| s.trim().to_uppercase());
            let nat_cult = opt_str(nature_culture, row).map(|s| s.trim().to_uppercase());
            let nat_cult_sp =
                opt_str(nature_culture_speciale, row).map(|s| s.trim().to_uppercase());

            // code_postal left-pad 5
            let cpostal_norm = opt_str(code_postal, row).map(left_pad_5);

            // Hash key
            let parcelle = opt_str(id_parcelle, row).map(str::trim);
            let addr_voie = opt_str(adresse_nom_voie, row).map(str::trim);
            let mut hasher = Hasher::new();
            hasher.update(id_mut.as_bytes());
            hasher.update(b"|");
            // date mutation
            hasher.update(date_s.as_bytes());
            hasher.update(b"|");
            hasher.update(num_disp.as_bytes());
            hasher.update(b"|");
            if let Some(p) = parcelle {
                hasher.update(p.as_bytes());
            }
            hasher.update(b"|");
            if let Some(id_p) = id_parcelle{
                hasher.update(id_p.value_data());
            }
            hasher.update(b"|");
            if let Some(adresse_numero) = adresse_numero{
                hasher.update(adresse_numero.value_data());
            }

            hasher.update(b"|");
            if let Some(v) = addr_voie {
                hasher.update(v.as_bytes());
            }
            hasher.update(b"|");
            if let Some(cp) = cpostal_norm.as_deref() {
                hasher.update(cp.as_bytes());
            }
            let key32: [u8; 32] = *hasher.finalize().as_bytes();

            // Dedup
            if !seen_keys.insert(key32) {
                continue;
            }

            // ---- Append one Silver row ----


            b.id_mutation.append_value(id_mut);
            b.date_mutation_days.append_value(date_days);
            b.numero_disposition.append_value(num_disp);
            b.nature_mutation.append_value(&nat_mut);



            if has_valeur {
                b.valeur_fonciere.append_value(valeur_scaled);
            } else {
                b.valeur_fonciere.append_null();
            }

            append_opt_utf8_to(&mut b.adresse_numero, adresse_numero, row);
            append_opt_utf8_to(&mut b.adresse_suffixe, adresse_suffixe, row);
            append_opt_utf8_to(&mut b.adresse_nom_voie, adresse_nom_voie, row);
            append_opt_utf8_to(&mut b.adresse_code_voie, adresse_code_voie, row);

            if let Some(cp) = cpostal_norm.as_deref() {
                b.code_postal.append_value(cp);
            } else {
                append_opt_utf8_to(&mut b.code_postal, code_postal, row);
            }

            append_opt_utf8_to(&mut b.code_commune, code_commune, row);
            if let Some(nc) = nom_com.as_deref() {
                b.nom_commune.append_value(nc);
            } else {
                append_opt_utf8_to(&mut b.nom_commune, nom_commune, row);
            }

            append_opt_utf8_to(&mut b.code_departement, code_departement, row);
            append_opt_utf8_to(&mut b.ancien_code_commune, ancien_code_commune, row);
            append_opt_utf8_to(&mut b.ancien_nom_commune, ancien_nom_commune, row);
            append_opt_utf8_to(&mut b.id_parcelle, id_parcelle, row);
            append_opt_utf8_to(&mut b.ancien_id_parcelle, ancien_id_parcelle, row);
            append_opt_utf8_to(&mut b.numero_volume, numero_volume, row);

            append_opt_utf8_to(&mut b.lot1_numero, lot1_numero, row);
            append_opt_f64_to(&mut b.lot1_surface_carrez, lot1_surface_carrez, row, true);
            append_opt_utf8_to(&mut b.lot2_numero, lot2_numero, row);
            append_opt_f64_to(&mut b.lot2_surface_carrez, lot2_surface_carrez, row, true);
            append_opt_utf8_to(&mut b.lot3_numero, lot3_numero, row);
            append_opt_f64_to(&mut b.lot3_surface_carrez, lot3_surface_carrez, row, true);
            append_opt_utf8_to(&mut b.lot4_numero, lot4_numero, row);
            append_opt_f64_to(&mut b.lot4_surface_carrez, lot4_surface_carrez, row, true);
            append_opt_utf8_to(&mut b.lot5_numero, lot5_numero, row);
            append_opt_f64_to(&mut b.lot5_surface_carrez, lot5_surface_carrez, row, true);

            append_opt_i16_to(&mut b.nombre_lots, nombre_lots, row, true);
            append_opt_utf8_to(&mut b.code_type_local, code_type_local, row);
            if let Some(tl) = typ_loc.as_deref() {
                b.type_local.append_value(tl);
            } else {
                append_opt_utf8_to(&mut b.type_local, type_local, row);
            }

            append_opt_i32_to(&mut b.surface_reelle_bati, surface_reelle_bati, row, true);
            append_opt_i16_to(
                &mut b.nombre_pieces_principales,
                nombre_pieces_principales,
                row,
                true,
            );
            append_opt_utf8_to(&mut b.code_nature_culture, code_nature_culture, row);
            if let Some(nc) = nat_cult.as_deref() {
                b.nature_culture.append_value(nc);
            } else {
                append_opt_utf8_to(&mut b.nature_culture, nature_culture, row);
            }

            append_opt_utf8_to(
                &mut b.code_nature_culture_speciale,
                code_nature_culture_speciale,
                row,
            );
            if let Some(ns) = nat_cult_sp.as_deref() {
                b.nature_culture_speciale.append_value(ns);
            } else {
                append_opt_utf8_to(&mut b.nature_culture_speciale, nature_culture_speciale, row);
            }

            append_opt_i32_to(&mut b.surface_terrain, surface_terrain, row, true);

            // lon/lat
            match (lon_opt, lat_opt) {
                (Some(lon), Some(lat)) => {
                    b.longitude.append_value(lon);
                    b.latitude.append_value(lat);
                }
                _ => {
                    b.longitude.append_null();
                    b.latitude.append_null();
                }
            }

            // Derived
            b.year_mutation.append_value(y as i16);
            if let (Some(lon), Some(lat)) = (lon_opt, lat_opt) {
                let gh = geohash::encode(
                    geohash::Coord { x: lon, y: lat },
                    6 // précision ~1.2km
                ).ok();
                if let Some(s) = gh { b.geohash6.append_value(&s); } else { b.geohash6.append_null(); }
            } else {
                b.geohash6.append_null();
            }
            b.mutation_key.append_value(&key32)?;

            match prix_m2_val {
                Some(v) if v.is_finite() && v > 0.0 => b.prix_m2.append_value(v),
                _ => b.prix_m2.append_null(),
            }
            let month_start = Date::from_calendar_date(date_parsed.year(), date_parsed.month(), 1)?;
            let month_start_days = date32_from_date(month_start);
            b.month_start.append_value(month_start_days);
            rows_out += 1;
            batch_rows += 1;

            if batch_rows == BATCH_SIZE {
                silver_writer.write(&b.finish_batch(&silver_schema))?;
                batch_rows = 0;
                b = SilverBuilders::try_new(BATCH_SIZE)?;
            }
        }
    }

    // Flush
    if batch_rows > 0 {
        silver_writer.write(&b.finish_batch(&silver_schema))?;
    }
    if r.len() > 0 {
        rejects_writer.write(&r.finish_batch())?;
    }

    silver_writer.finish()?;
    rejects_writer.finish()?;

    Ok(ValidationStats {
        rows_in,
        rows_out,
        rejects,
        silver_out,
        rejects_out,
    })
}

// -------------------- helpers & structures --------------------

const BATCH_SIZE: usize = 65_536;

struct BronzeIdx {
    // required
    idx_id_mutation: usize,
    idx_date_mutation: usize,
    idx_numero_disposition: usize,
    idx_nature_mutation: usize,
    // optionals
    idx_valeur_fonciere: usize,
    idx_adresse_numero: usize,
    idx_adresse_suffixe: usize,
    idx_adresse_nom_voie: usize,
    idx_adresse_code_voie: usize,
    idx_code_postal: usize,
    idx_code_commune: usize,
    idx_nom_commune: usize,
    idx_code_departement: usize,
    idx_ancien_code_commune: usize,
    idx_ancien_nom_commune: usize,
    idx_id_parcelle: usize,
    idx_ancien_id_parcelle: usize,
    idx_numero_volume: usize,
    idx_lot1_numero: usize,
    idx_lot1_surface_carrez: usize,
    idx_lot2_numero: usize,
    idx_lot2_surface_carrez: usize,
    idx_lot3_numero: usize,
    idx_lot3_surface_carrez: usize,
    idx_lot4_numero: usize,
    idx_lot4_surface_carrez: usize,
    idx_lot5_numero: usize,
    idx_lot5_surface_carrez: usize,
    idx_nombre_lots: usize,
    idx_code_type_local: usize,
    idx_type_local: usize,
    idx_surface_reelle_bati: usize,
    idx_nombre_pieces_principales: usize,
    idx_code_nature_culture: usize,
    idx_nature_culture: usize,
    idx_code_nature_culture_speciale: usize,
    idx_nature_culture_speciale: usize,
    idx_surface_terrain: usize,
    idx_longitude: usize,
    idx_latitude: usize,
    // lineage
    idx_ingest_date: usize,
    idx_source_file: usize,
    idx_row_number: usize,
}

impl BronzeIdx {
    fn from_schema(schema: &Arc<Schema>) -> Result<Self> {
        let g = |name: &str| {
            schema
                .index_of(name)
                .with_context(|| format!("missing column in Bronze: {name}"))
        };
        Ok(Self {
            idx_id_mutation: g("id_mutation")?,
            idx_date_mutation: g("date_mutation")?,
            idx_numero_disposition: g("numero_disposition")?,
            idx_nature_mutation: g("nature_mutation")?,
            idx_valeur_fonciere: g("valeur_fonciere")?,
            idx_adresse_numero: g("adresse_numero")?,
            idx_adresse_suffixe: g("adresse_suffixe")?,
            idx_adresse_nom_voie: g("adresse_nom_voie")?,
            idx_adresse_code_voie: g("adresse_code_voie")?,
            idx_code_postal: g("code_postal")?,
            idx_code_commune: g("code_commune")?,
            idx_nom_commune: g("nom_commune")?,
            idx_code_departement: g("code_departement")?,
            idx_ancien_code_commune: g("ancien_code_commune")?,
            idx_ancien_nom_commune: g("ancien_nom_commune")?,
            idx_id_parcelle: g("id_parcelle")?,
            idx_ancien_id_parcelle: g("ancien_id_parcelle")?,
            idx_numero_volume: g("numero_volume")?,
            idx_lot1_numero: g("lot1_numero")?,
            idx_lot1_surface_carrez: g("lot1_surface_carrez")?,
            idx_lot2_numero: g("lot2_numero")?,
            idx_lot2_surface_carrez: g("lot2_surface_carrez")?,
            idx_lot3_numero: g("lot3_numero")?,
            idx_lot3_surface_carrez: g("lot3_surface_carrez")?,
            idx_lot4_numero: g("lot4_numero")?,
            idx_lot4_surface_carrez: g("lot4_surface_carrez")?,
            idx_lot5_numero: g("lot5_numero")?,
            idx_lot5_surface_carrez: g("lot5_surface_carrez")?,
            idx_nombre_lots: g("nombre_lots")?,
            idx_code_type_local: g("code_type_local")?,
            idx_type_local: g("type_local")?,
            idx_surface_reelle_bati: g("surface_reelle_bati")?,
            idx_nombre_pieces_principales: g("nombre_pieces_principales")?,
            idx_code_nature_culture: g("code_nature_culture")?,
            idx_nature_culture: g("nature_culture")?,
            idx_code_nature_culture_speciale: g("code_nature_culture_speciale")?,
            idx_nature_culture_speciale: g("nature_culture_speciale")?,
            idx_surface_terrain: g("surface_terrain")?,
            idx_longitude: g("longitude")?,
            idx_latitude: g("latitude")?,
            idx_ingest_date: g("ingest_date")?,
            idx_source_file: g("source_file")?,
            idx_row_number: g("row_number")?,

        })
    }
    fn get(&self, name: &str) -> Result<usize> {
        Ok(match name {
            "id_mutation" => self.idx_id_mutation,
            "date_mutation" => self.idx_date_mutation,
            "numero_disposition" => self.idx_numero_disposition,
            "nature_mutation" => self.idx_nature_mutation,
            "ingest_date" => self.idx_ingest_date,
            "source_file" => self.idx_source_file,
            other => return Err(anyhow!("unknown bronze column: {other}")),
        })
    }
}

// ---------- Silver schema & Rejects schema

fn silver_schema() -> Schema {
    use DataType::*;
    Schema::new(vec![
        Field::new("id_mutation", Utf8, false),
        Field::new("date_mutation", Date32, false),
        Field::new("numero_disposition", Utf8, false),
        Field::new("nature_mutation", Utf8, false),
        Field::new("valeur_fonciere", Decimal128(12, 2), true),
        Field::new("adresse_numero", Utf8, true),
        Field::new("adresse_suffixe", Utf8, true),
        Field::new("adresse_nom_voie", Utf8, true),
        Field::new("adresse_code_voie", Utf8, true),
        Field::new("code_postal", Utf8, true),
        Field::new("code_commune", Utf8, true),
        Field::new("nom_commune", Utf8, true),
        Field::new("code_departement", Utf8, true),
        Field::new("ancien_code_commune", Utf8, true),
        Field::new("ancien_nom_commune", Utf8, true),
        Field::new("id_parcelle", Utf8, true),
        Field::new("ancien_id_parcelle", Utf8, true),
        Field::new("numero_volume", Utf8, true),
        Field::new("lot1_numero", Utf8, true),
        Field::new("lot1_surface_carrez", Float64, true),
        Field::new("lot2_numero", Utf8, true),
        Field::new("lot2_surface_carrez", Float64, true),
        Field::new("lot3_numero", Utf8, true),
        Field::new("lot3_surface_carrez", Float64, true),
        Field::new("lot4_numero", Utf8, true),
        Field::new("lot4_surface_carrez", Float64, true),
        Field::new("lot5_numero", Utf8, true),
        Field::new("lot5_surface_carrez", Float64, true),
        Field::new("nombre_lots", DataType::Int16, true),
        Field::new("code_type_local", Utf8, true),
        Field::new("type_local", Utf8, true),
        Field::new("surface_reelle_bati", DataType::Int32, true),
        Field::new("nombre_pieces_principales", DataType::Int16, true),
        Field::new("code_nature_culture", Utf8, true),
        Field::new("nature_culture", Utf8, true),
        Field::new("code_nature_culture_speciale", Utf8, true),
        Field::new("nature_culture_speciale", Utf8, true),
        Field::new("surface_terrain", DataType::Int32, true),
        Field::new("longitude", Float64, true),
        Field::new("latitude", Float64, true),
        Field::new("year_mutation", DataType::Int16, false),
        Field::new("geohash6", Utf8, true),
        Field::new("mutation_key", DataType::FixedSizeBinary(32), false),
        Field::new("prix_m2", DataType::Float64, true),
        Field::new("month_start", DataType::Date32, false),
    ])
}

fn rejects_schema(bronze: &Schema) -> Result<Schema> {
    // All original Bronze columns rewritten as Utf8 (nullable) + error fields.
    let mut fields: Vec<Field> = bronze
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), DataType::Utf8, true))
        .collect();
    fields.push(Field::new("error_code", DataType::Utf8, false));
    fields.push(Field::new("error_detail", DataType::Utf8, false));
    fields.push(Field::new("validation_stage", DataType::Utf8, false));
    Ok(Schema::new(fields))
}

// ---------- Builders

struct SilverBuilders {
    id_mutation: StringBuilder,
    date_mutation_days: Date32Builder,
    numero_disposition: StringBuilder,
    nature_mutation: StringBuilder,
    valeur_fonciere: Decimal128Builder,
    adresse_numero: StringBuilder,
    adresse_suffixe: StringBuilder,
    adresse_nom_voie: StringBuilder,
    adresse_code_voie: StringBuilder,
    code_postal: StringBuilder,
    code_commune: StringBuilder,
    nom_commune: StringBuilder,
    code_departement: StringBuilder,
    ancien_code_commune: StringBuilder,
    ancien_nom_commune: StringBuilder,
    id_parcelle: StringBuilder,
    ancien_id_parcelle: StringBuilder,
    numero_volume: StringBuilder,
    lot1_numero: StringBuilder,
    lot1_surface_carrez: Float64Builder,
    lot2_numero: StringBuilder,
    lot2_surface_carrez: Float64Builder,
    lot3_numero: StringBuilder,
    lot3_surface_carrez: Float64Builder,
    lot4_numero: StringBuilder,
    lot4_surface_carrez: Float64Builder,
    lot5_numero: StringBuilder,
    lot5_surface_carrez: Float64Builder,
    nombre_lots: Int16Builder,
    code_type_local: StringBuilder,
    type_local: StringBuilder,
    surface_reelle_bati: Int32Builder,
    nombre_pieces_principales: Int16Builder,
    code_nature_culture: StringBuilder,
    nature_culture: StringBuilder,
    code_nature_culture_speciale: StringBuilder,
    nature_culture_speciale: StringBuilder,
    surface_terrain: Int32Builder,
    longitude: Float64Builder,
    latitude: Float64Builder,
    year_mutation: Int16Builder,
    geohash6: StringBuilder,
    mutation_key: FixedSizeBinaryBuilder,
    prix_m2: Float64Builder,
    month_start: Date32Builder
}

impl SilverBuilders {
    fn try_new(cap: usize) -> Result<Self, arrow::error::ArrowError> {
        Ok(Self {
            id_mutation: StringBuilder::with_capacity(cap, cap * 16),
            date_mutation_days: Date32Builder::with_capacity(cap),
            numero_disposition: StringBuilder::with_capacity(cap, cap * 8),
            nature_mutation: StringBuilder::with_capacity(cap, cap * 8),
            valeur_fonciere: Decimal128Builder::with_capacity(cap)
                .with_precision_and_scale(12, 2)?,
            adresse_numero: StringBuilder::with_capacity(cap, cap * 4),
            adresse_suffixe: StringBuilder::with_capacity(cap, cap * 4),
            adresse_nom_voie: StringBuilder::with_capacity(cap, cap * 16),
            adresse_code_voie: StringBuilder::with_capacity(cap, cap * 8),
            code_postal: StringBuilder::with_capacity(cap, cap * 8),
            code_commune: StringBuilder::with_capacity(cap, cap * 8),
            nom_commune: StringBuilder::with_capacity(cap, cap * 16),
            code_departement: StringBuilder::with_capacity(cap, cap * 4),
            ancien_code_commune: StringBuilder::with_capacity(cap, cap * 4),
            ancien_nom_commune: StringBuilder::with_capacity(cap, cap * 16),
            id_parcelle: StringBuilder::with_capacity(cap, cap * 16),
            ancien_id_parcelle: StringBuilder::with_capacity(cap, cap * 16),
            numero_volume: StringBuilder::with_capacity(cap, cap * 4),
            lot1_numero: StringBuilder::with_capacity(cap, cap * 8),
            lot1_surface_carrez: Float64Builder::with_capacity(cap),
            lot2_numero: StringBuilder::with_capacity(cap, cap * 8),
            lot2_surface_carrez: Float64Builder::with_capacity(cap),
            lot3_numero: StringBuilder::with_capacity(cap, cap * 8),
            lot3_surface_carrez: Float64Builder::with_capacity(cap),
            lot4_numero: StringBuilder::with_capacity(cap, cap * 8),
            lot4_surface_carrez: Float64Builder::with_capacity(cap),
            lot5_numero: StringBuilder::with_capacity(cap, cap * 8),
            lot5_surface_carrez: Float64Builder::with_capacity(cap),
            nombre_lots: Int16Builder::with_capacity(cap),
            code_type_local: StringBuilder::with_capacity(cap, cap * 4),
            type_local: StringBuilder::with_capacity(cap, cap * 8),
            surface_reelle_bati: Int32Builder::with_capacity(cap),
            nombre_pieces_principales: Int16Builder::with_capacity(cap),
            code_nature_culture: StringBuilder::with_capacity(cap, cap * 4),
            nature_culture: StringBuilder::with_capacity(cap, cap * 8),
            code_nature_culture_speciale: StringBuilder::with_capacity(cap, cap * 8),
            nature_culture_speciale: StringBuilder::with_capacity(cap, cap * 8),
            surface_terrain: Int32Builder::with_capacity(cap),
            longitude: Float64Builder::with_capacity(cap),
            latitude: Float64Builder::with_capacity(cap),
            year_mutation: Int16Builder::with_capacity(cap),
            geohash6: StringBuilder::with_capacity(cap, cap * 8),
            mutation_key: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            prix_m2: Float64Builder::with_capacity(cap),
            month_start: Date32Builder::with_capacity(cap),

        })
    }

    fn finish_batch(&mut self, schema: &Arc<Schema>) -> RecordBatch {
        macro_rules! finish {
            ($b:expr) => {
                Arc::new($b.finish()) as ArrayRef
            };
        }
        let cols: Vec<ArrayRef> = vec![
            finish!(self.id_mutation),
            finish!(self.date_mutation_days),
            finish!(self.numero_disposition),
            finish!(self.nature_mutation),
            finish!(self.valeur_fonciere),
            finish!(self.adresse_numero),
            finish!(self.adresse_suffixe),
            finish!(self.adresse_nom_voie),
            finish!(self.adresse_code_voie),
            finish!(self.code_postal),
            finish!(self.code_commune),
            finish!(self.nom_commune),
            finish!(self.code_departement),
            finish!(self.ancien_code_commune),
            finish!(self.ancien_nom_commune),
            finish!(self.id_parcelle),
            finish!(self.ancien_id_parcelle),
            finish!(self.numero_volume),
            finish!(self.lot1_numero),
            finish!(self.lot1_surface_carrez),
            finish!(self.lot2_numero),
            finish!(self.lot2_surface_carrez),
            finish!(self.lot3_numero),
            finish!(self.lot3_surface_carrez),
            finish!(self.lot4_numero),
            finish!(self.lot4_surface_carrez),
            finish!(self.lot5_numero),
            finish!(self.lot5_surface_carrez),
            finish!(self.nombre_lots),
            finish!(self.code_type_local),
            finish!(self.type_local),
            finish!(self.surface_reelle_bati),
            finish!(self.nombre_pieces_principales),
            finish!(self.code_nature_culture),
            finish!(self.nature_culture),
            finish!(self.code_nature_culture_speciale),
            finish!(self.nature_culture_speciale),
            finish!(self.surface_terrain),
            finish!(self.longitude),
            finish!(self.latitude),
            finish!(self.year_mutation),
            finish!(self.geohash6),
            finish!(self.mutation_key),
            finish!(self.prix_m2),
            finish!(self.month_start)
        ];
        RecordBatch::try_new(schema.clone(), cols).unwrap()
    }
}

// ---------- Rejects builder

struct RejectBuilders {
    schema: Arc<Schema>,
    cols: Vec<StringBuilder>,
    error_code: StringBuilder,
    error_detail: StringBuilder,
    validation_stage: StringBuilder,
    pending: usize,
}

impl RejectBuilders {
    fn new(schema: Arc<Schema>,bronze_fields_len: usize,  cap: usize) -> Result<Self> {
        let cols = (0..bronze_fields_len)
            .map(|_| StringBuilder::with_capacity(cap, cap * 16))
            .collect::<Vec<_>>();
        Ok(Self {
            schema,
            cols,
            error_code: StringBuilder::with_capacity(cap, cap * 8),
            error_detail: StringBuilder::with_capacity(cap, cap * 16),
            validation_stage: StringBuilder::with_capacity(cap, cap * 8),
            pending: 0,
        })
    }
    fn push_reject(
        &mut self,
        bronze: &RecordBatch,
        row: usize,
        code: &str,
        detail: &str,
    ) -> Result<()> {
        // Copy all Bronze columns as Utf8 (best-effort)
        for (i, b) in self.cols.iter_mut().enumerate() {
            let arr = bronze.column(i);
            if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
                if s.is_null(row) {
                    b.append_null();
                } else {
                    b.append_value(s.value(row));
                }
            } else if let Some(i64a) = arr.as_any().downcast_ref::<Int64Array>() {
                if i64a.is_null(row) {
                    b.append_null();
                } else {
                    b.append_value(&i64a.value(row).to_string());
                }
            } else if let Some(i32a) = arr.as_any().downcast_ref::<Int32Array>() {
                if i32a.is_null(row) {
                    b.append_null();
                } else {
                    b.append_value(&i32a.value(row).to_string());
                }
            } else {
                b.append_null();
            }
        }
        self.error_code.append_value(code);
        self.error_detail.append_value(detail);
        self.validation_stage.append_value("silver");
        self.pending += 1;
        Ok(())
    }
    fn len(&self) -> usize {
        self.pending
    }
    fn finish_batch(&mut self) -> RecordBatch {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.cols.len() + 3);
        for c in self.cols.iter_mut() {
            arrays.push(Arc::new(c.finish()) as ArrayRef);
        }
        arrays.push(Arc::new(self.error_code.finish()) as ArrayRef);
        arrays.push(Arc::new(self.error_detail.finish()) as ArrayRef);
        arrays.push(Arc::new(self.validation_stage.finish()) as ArrayRef);
        self.pending = 0;
        RecordBatch::try_new(self.schema.clone(), arrays).unwrap()
    }
}

// ---------- small helpers

fn as_utf8_opt(batch: &RecordBatch, col_idx: usize) -> Option<&StringArray> {
    batch.column(col_idx).as_any().downcast_ref::<StringArray>()
}
fn opt_str(arr_opt: Option<&StringArray>, row: usize) -> Option<&str> {
    arr_opt.and_then(|a| {
        if a.is_null(row) {
            None
        } else {
            Some(a.value(row))
        }
    })
}

fn append_opt_utf8_to(tgt: &mut StringBuilder, arr_opt: Option<&StringArray>, row: usize) {
    match opt_str(arr_opt, row) {
        Some(s) if !s.trim().is_empty() => tgt.append_value(s.trim()),
        _ => tgt.append_null(),
    }
}
fn append_opt_f64_to(
    tgt: &mut Float64Builder,
    arr_opt: Option<&StringArray>,
    row: usize,
    non_negative: bool,
) {
    match opt_str(arr_opt, row).and_then(parse_f64) {
        Some(v) if !non_negative || v >= 0.0 => tgt.append_value(v),
        Some(_) => tgt.append_null(),
        None => tgt.append_null(),
    }
}
fn append_opt_i16_to(
    tgt: &mut Int16Builder,
    arr_opt: Option<&StringArray>,
    row: usize,
    non_negative: bool,
) {
    match opt_str(arr_opt, row) {
        Some(s) => {
            if let Ok(v) = s.trim().parse::<i32>() {
                if v >= 0 || !non_negative {
                    if v <= i16::MAX as i32 {
                        tgt.append_value(v as i16);
                    } else {
                        tgt.append_null();
                    }
                } else {
                    tgt.append_null();
                }
            } else {
                tgt.append_null();
            }
        }
        None => tgt.append_null(),
    }
}
fn append_opt_i32_to(
    tgt: &mut Int32Builder,
    arr_opt: Option<&StringArray>,
    row: usize,
    non_negative: bool,
) {
    match opt_str(arr_opt, row) {
        Some(s) => {
            if let Ok(v) = s.trim().parse::<i64>() {
                if v >= 0 || !non_negative {
                    if v <= i32::MAX as i64 {
                        tgt.append_value(v as i32);
                    } else {
                        tgt.append_null();
                    }
                } else {
                    tgt.append_null();
                }
            } else {
                tgt.append_null();
            }
        }
        None => tgt.append_null(),
    }
}

fn parse_f64(s: &str) -> Option<f64> {
    let t = s.trim().replace(',', ".");
    t.parse::<f64>().ok()
}
fn parse_decimal_2(s: &str) -> Option<i128> {
    let t = s.trim().replace(',', ".");
    let f = t.parse::<f64>().ok()?;
    let cents = (f * 100.0).round();
    if cents.is_finite() {
        Some(cents as i128)
    } else {
        None
    }
}
fn left_pad_5(s: &str) -> String {
    let mut t = s.trim().to_string();
    while t.len() < 5 {
        t.insert(0, '0');
    }
    t
}
fn date32_from_date(d: Date) -> i32 {
    let epoch = Date::from_calendar_date(1970, Month::January, 1).unwrap();
    (d - epoch).whole_days() as i32
}

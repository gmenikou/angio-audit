import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st


APP_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
CONFIG_FILE = APP_DIR / "db_config.json"

DEFAULT_DB_CONFIG = {
    "dbname": "",
    "user": "",
    "password": "",
    "host": "",
    "port": "5432",
}

SYSTEM_SQL_EXPR = """
CASE
    WHEN NULLIF(BTRIM(ge.station_name), '') IS NOT NULL
         AND NULLIF(BTRIM(ge.manufacturer_model_name), '') IS NOT NULL
         AND NULLIF(BTRIM(ge.device_serial_number), '') IS NOT NULL
    THEN ge.station_name || ' | ' || ge.manufacturer_model_name || ' | ' || ge.device_serial_number
    WHEN NULLIF(BTRIM(ge.station_name), '') IS NOT NULL
         AND NULLIF(BTRIM(ge.manufacturer_model_name), '') IS NOT NULL
    THEN ge.station_name || ' | ' || ge.manufacturer_model_name
    WHEN NULLIF(BTRIM(ge.station_name), '') IS NOT NULL
         AND NULLIF(BTRIM(ge.device_serial_number), '') IS NOT NULL
    THEN ge.station_name || ' | ' || ge.device_serial_number
    WHEN NULLIF(BTRIM(ge.manufacturer_model_name), '') IS NOT NULL
         AND NULLIF(BTRIM(ge.device_serial_number), '') IS NOT NULL
    THEN ge.manufacturer_model_name || ' | ' || ge.device_serial_number
    ELSE COALESCE(
        NULLIF(BTRIM(ge.station_name), ''),
        NULLIF(BTRIM(ge.manufacturer_model_name), ''),
        NULLIF(BTRIM(ge.device_serial_number), ''),
        'Unknown system'
    )
END
"""

FIRST_PHYSICIAN_SQL_EXPR = """
NULLIF(BTRIM(SPLIT_PART(COALESCE(gs.performing_physician_name, ''), '|', 1)), '')
"""

EVENT_VIEW_COLUMNS = {
    "A": [
        "first_physician",
        "system_name",
        "accession_number",
        "irradiation_event_id",
        "date_time_started",
        "acquisition_protocol",
        "dose_area_product",
        "dose_rp",
        "irradiation_duration",
        "field_area_cm2",
        "estimated_field_area_at_sod_cm2",
        "estimated_entrance_field_area_cm2",
        "field_usage_percent",
        "collimation_change_flag",
    ],
    "B": [
        "first_physician",
        "system_name",
        "accession_number",
        "irradiation_event_id",
        "irradiation_event_uid",
        "date_time_started",
        "acquisition_protocol",
        "dose_area_product",
        "dose_rp",
        "irradiation_duration",
        "pulse_rate",
        "number_of_pulses",
        "patient_equivalent_thickness",
        "positioner_primary_angle",
        "positioner_secondary_angle",
        "distance_source_to_isocenter",
        "distance_source_to_detector",
        "distance_source_to_entrance_surface",
        "collimated_field_width",
        "collimated_field_height",
        "field_area_cm2",
        "estimated_field_area_at_sod_cm2",
        "fov_mm",
        "fov_side_cm",
        "max_fov_area_cm2",
        "field_usage_percent",
        "collimation_change_score",
        "collimation_change_flag",
        "estimated_entrance_field_area_cm2",
    ],
    "C": None,
}

SUMMARY_VIEW_COLUMNS = {
    "A": [
        "first_physician",
        "system_name",
        "n_events",
        "mean_dap",
        "mean_dose_rp",
        "mean_irradiation_duration",
        "mean_field_area_cm2",
        "mean_estimated_field_area_at_sod_cm2",
        "mean_estimated_entrance_field_area_cm2",
        "mean_field_usage_percent",
        "n_flagged_collimation_change_events",
    ],
    "B": [
        "first_physician",
        "system_name",
        "n_events",
        "total_dap",
        "mean_dap",
        "total_dose_rp",
        "mean_dose_rp",
        "total_irradiation_duration",
        "mean_irradiation_duration",
        "mean_pulses",
        "mean_pulse_rate",
        "mean_field_area_cm2",
        "max_field_area_cm2",
        "mean_estimated_field_area_at_sod_cm2",
        "max_estimated_field_area_at_sod_cm2",
        "mean_estimated_entrance_field_area_cm2",
        "max_estimated_entrance_field_area_cm2",
        "mean_field_usage_percent",
        "max_field_usage_percent",
        "n_flagged_collimation_change_events",
    ],
    "C": None,
}

DISPLAY_HEADER_MAP = {
    "accession_number": "accession_number",
    "dose_area_product": "dose_area_product (Gy·m²)",
    "dose_rp": "dose_rp (Gy)",
    "irradiation_duration": "irradiation_duration (s)",
    "collimated_field_width": "collimated_field_width (mm)",
    "collimated_field_height": "collimated_field_height (mm)",
    "field_area_cm2": "field_area@detector (cm²)",
    "distance_source_to_isocenter": "SOD / source_to_isocenter (mm)",
    "fov_mm": "FOV diagonal (mm)",
    "fov_side_cm": "FOV diagonal (cm)",
    "max_fov_area_cm2": "max_FOV_area (cm²)",
    "field_usage_percent": "FOV usage (%)",
    "mean_field_usage_percent": "mean_FOV_usage (%)",
    "max_field_usage_percent": "max_FOV_usage (%)",
    "distance_source_to_detector": "SID (mm)",
    "distance_source_to_entrance_surface": "SSD (mm)",
    "estimated_field_area_at_sod_cm2": "field_area@SOD (cm²)",
    "mean_estimated_field_area_at_sod_cm2": "mean_field_area@SOD (cm²)",
    "max_estimated_field_area_at_sod_cm2": "max_field_area@SOD (cm²)",
    "estimated_entrance_field_area_cm2": "field_area@SSD (cm²)",
    "mean_estimated_entrance_field_area_cm2": "mean_field_area@SSD (cm²)",
    "max_estimated_entrance_field_area_cm2": "max_field_area@SSD (cm²)",
    "patient_equivalent_thickness": "patient_equivalent_thickness (mm)",
    "positioner_primary_angle": "positioner_primary_angle (deg)",
    "positioner_secondary_angle": "positioner_secondary_angle (deg)",
}


@st.cache_data(show_spinner=False)
def load_file_db_config():
    if CONFIG_FILE.exists():
        try:
            data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            return {
                "dbname": data.get("dbname", ""),
                "user": data.get("user", ""),
                "password": "",
                "host": data.get("host", ""),
                "port": str(data.get("port", "5432")),
            }
        except Exception:
            return DEFAULT_DB_CONFIG.copy()
    return DEFAULT_DB_CONFIG.copy()


def load_db_config():
    try:
        if "db" in st.secrets:
            return {
                "dbname": st.secrets["db"].get("dbname", ""),
                "user": st.secrets["db"].get("user", ""),
                "password": st.secrets["db"].get("password", ""),
                "host": st.secrets["db"].get("host", ""),
                "port": str(st.secrets["db"].get("port", "5432")),
            }
    except Exception:
        pass
    return load_file_db_config()


def save_db_config(config):
    safe_config = {
        "dbname": config.get("dbname", ""),
        "user": config.get("user", ""),
        "host": config.get("host", ""),
        "port": config.get("port", "5432"),
    }
    CONFIG_FILE.write_text(json.dumps(safe_config, indent=2), encoding="utf-8")
    load_file_db_config.clear()


@st.cache_resource(show_spinner=False)
def get_connection(dbname, user, password, host, port):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )


def clear_connection_cache():
    get_connection.clear()


def run_query(config, sql, params=None):
    conn = get_connection(
        config["dbname"],
        config["user"],
        config["password"],
        config["host"],
        config["port"],
    )
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
    return headers, rows


def validate_date(date_text: str) -> bool:
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def filter_columns(headers, rows, view_columns):
    if view_columns is None:
        return headers[:], rows[:]
    keep_cols = [c for c in view_columns if c in headers]
    idxs = [headers.index(c) for c in keep_cols]
    filtered_rows = [[row[i] for i in idxs] for row in rows]
    return keep_cols, filtered_rows


def remove_empty_columns(headers, rows):
    if not headers or not rows:
        return headers, rows

    keep_idxs = []
    keep_headers = []

    for idx, header in enumerate(headers):
        has_value = any(
            idx < len(row) and row[idx] is not None and str(row[idx]).strip() != ""
            for row in rows
        )
        if has_value:
            keep_idxs.append(idx)
            keep_headers.append(header)

    filtered_rows = [[row[i] for i in keep_idxs] for row in rows]
    return keep_headers, filtered_rows


def to_dataframe(headers, rows):
    df = pd.DataFrame(rows, columns=headers)
    return df.rename(columns={c: DISPLAY_HEADER_MAP.get(c, c) for c in df.columns})


def parse_multi_search_terms(text):
    if not text:
        return []
    normalized = text.replace("\n", ",").replace(";", ",")
    parts = []
    for chunk in normalized.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.extend([p.strip() for p in chunk.split() if p.strip()])

    seen = set()
    result = []
    for p in parts:
        p_low = p.lower()
        if p_low not in seen:
            seen.add(p_low)
            result.append(p_low)
    return result


def filter_events_by_accession(df, search_text):
    if df.empty or not search_text or "accession_number" not in df.columns:
        return df

    terms = parse_multi_search_terms(search_text)
    if not terms:
        return df

    mask = df["accession_number"].fillna("").astype(str).str.lower().apply(
        lambda v: any(term in v for term in terms)
    )
    return df[mask].copy()


def filter_summary_by_text(df, search_text):
    if df.empty or not search_text.strip():
        return df
    q = search_text.strip().lower()
    mask = df.astype(str).apply(lambda col: col.str.lower().str.contains(q, na=False))
    return df[mask.any(axis=1)].copy()


def mean_ignore_none(series):
    s = pd.to_numeric(series, errors="coerce").dropna()
    return 0 if s.empty else float(s.mean())


def sum_ignore_none(series):
    s = pd.to_numeric(series, errors="coerce").dropna()
    return 0 if s.empty else float(s.sum())


def max_ignore_none(series):
    s = pd.to_numeric(series, errors="coerce").dropna()
    return 0 if s.empty else float(s.max())


def build_summary_from_df(df):
    if df.empty:
        return pd.DataFrame()

    first_physician = next(
        (v for v in df.get("first_physician", pd.Series(dtype=object)).tolist() if str(v).strip() != ""),
        "",
    )
    system_name = next(
        (v for v in df.get("system_name", pd.Series(dtype=object)).tolist() if str(v).strip() != ""),
        "",
    )

    n_flagged = 0
    if "collimation_change_flag" in df.columns:
        n_flagged = int(
            df["collimation_change_flag"].fillna("").astype(str).isin(["REVIEW", "LIKELY"]).sum()
        )

    out = {
        "first_physician": first_physician,
        "system_name": system_name,
        "n_events": len(df),
        "total_dap": round(sum_ignore_none(df.get("dose_area_product", pd.Series(dtype=float))), 6),
        "mean_dap": round(mean_ignore_none(df.get("dose_area_product", pd.Series(dtype=float))), 6),
        "total_dose_rp": round(sum_ignore_none(df.get("dose_rp", pd.Series(dtype=float))), 6),
        "mean_dose_rp": round(mean_ignore_none(df.get("dose_rp", pd.Series(dtype=float))), 6),
        "total_irradiation_duration": round(sum_ignore_none(df.get("irradiation_duration", pd.Series(dtype=float))), 4),
        "mean_irradiation_duration": round(mean_ignore_none(df.get("irradiation_duration", pd.Series(dtype=float))), 4),
        "mean_pulses": round(mean_ignore_none(df.get("number_of_pulses", pd.Series(dtype=float))), 4),
        "mean_pulse_rate": round(mean_ignore_none(df.get("pulse_rate", pd.Series(dtype=float))), 4),
        "mean_field_area_cm2": round(mean_ignore_none(df.get("field_area_cm2", pd.Series(dtype=float))), 4),
        "max_field_area_cm2": round(max_ignore_none(df.get("field_area_cm2", pd.Series(dtype=float))), 4),
        "mean_estimated_field_area_at_sod_cm2": round(mean_ignore_none(df.get("estimated_field_area_at_sod_cm2", pd.Series(dtype=float))), 4),
        "max_estimated_field_area_at_sod_cm2": round(max_ignore_none(df.get("estimated_field_area_at_sod_cm2", pd.Series(dtype=float))), 4),
        "mean_estimated_entrance_field_area_cm2": round(mean_ignore_none(df.get("estimated_entrance_field_area_cm2", pd.Series(dtype=float))), 4),
        "max_estimated_entrance_field_area_cm2": round(max_ignore_none(df.get("estimated_entrance_field_area_cm2", pd.Series(dtype=float))), 4),
        "mean_field_usage_percent": round(mean_ignore_none(df.get("field_usage_percent", pd.Series(dtype=float))), 2),
        "max_field_usage_percent": round(max_ignore_none(df.get("field_usage_percent", pd.Series(dtype=float))), 2),
        "n_flagged_collimation_change_events": n_flagged,
    }
    return pd.DataFrame([out])


def collimation_category(mean_usage):
    try:
        v = float(mean_usage)
    except Exception:
        return "N/A"
    if v >= 80:
        return "BAD collimation"
    if v >= 50:
        return "AVERAGE collimation"
    return "GOOD collimation"


def style_events(df):
    if df.empty:
        return df

    def row_style(row):
        styles = [""] * len(row)
        usage = None
        flag = str(row.get("collimation_change_flag", "") or "").strip().upper()
        try:
            usage = float(row.get("field_usage_percent")) if row.get("field_usage_percent") not in (None, "") else None
        except Exception:
            usage = None

        color = None
        if flag == "LIKELY":
            color = "background-color: #ffd8a8"
        elif flag == "REVIEW":
            color = "background-color: #fff3bf"
        elif usage is not None:
            if usage >= 80:
                color = "background-color: #ffd6d6"
            elif usage >= 50:
                color = "background-color: #ffe6bf"
            else:
                color = "background-color: #d9f2d9"

        if color:
            styles = [color] * len(row)
        return styles

    return df.style.apply(row_style, axis=1)


def df_to_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8-sig")


def get_systems(config, date_from, date_to):
    _, rows = run_query(
        config,
        f"""
        SELECT DISTINCT system_name
        FROM (
            SELECT {SYSTEM_SQL_EXPR} AS system_name
            FROM remapp_irradeventxraydata ie
            JOIN remapp_projectionxrayradiationdose prd
              ON ie.projection_xray_radiation_dose_id = prd.id
            JOIN remapp_generalstudymoduleattr gs
              ON prd.general_study_module_attributes_id = gs.id
            LEFT JOIN remapp_generalequipmentmoduleattr ge
              ON ge.general_study_module_attributes_id = gs.id
            WHERE gs.study_date >= %s
              AND gs.study_date <= %s
        ) q
        WHERE system_name IS NOT NULL
          AND BTRIM(system_name) <> ''
        ORDER BY system_name;
        """,
        (date_from, date_to),
    )
    return [r[0] for r in rows]


def get_physicians(config, date_from, date_to, system_name):
    _, rows = run_query(
        config,
        f"""
        SELECT DISTINCT first_physician
        FROM (
            SELECT
                {FIRST_PHYSICIAN_SQL_EXPR} AS first_physician,
                {SYSTEM_SQL_EXPR} AS system_name
            FROM remapp_irradeventxraydata ie
            JOIN remapp_projectionxrayradiationdose prd
              ON ie.projection_xray_radiation_dose_id = prd.id
            JOIN remapp_generalstudymoduleattr gs
              ON prd.general_study_module_attributes_id = gs.id
            LEFT JOIN remapp_generalequipmentmoduleattr ge
              ON ge.general_study_module_attributes_id = gs.id
            WHERE gs.study_date >= %s
              AND gs.study_date <= %s
        ) q
        WHERE first_physician IS NOT NULL
          AND BTRIM(first_physician) <> ''
          AND system_name = %s
        ORDER BY first_physician;
        """,
        (date_from, date_to, system_name),
    )
    return [r[0] for r in rows]


def run_audit_query(config, date_from, date_to, system_name, physician_name):
    return run_query(
        config,
        f"""
        WITH base AS (
            SELECT
                {FIRST_PHYSICIAN_SQL_EXPR} AS first_physician,
                {SYSTEM_SQL_EXPR} AS system_name,
                gs.accession_number,
                ie.id AS irradiation_event_id,
                ie.irradiation_event_uid,
                ie.irradiation_event_label,
                ie.date_time_started,
                ie.acquisition_protocol,
                ie.projection_eponymous_name,
                ie.patient_table_relationship,
                ie.patient_orientation,
                ie.patient_orientation_modifier,
                ie.dose_area_product,
                ie.half_value_layer,
                ie.patient_equivalent_thickness,
                ie.entrance_exposure_at_rp,
                ie.reference_point_definition_text,
                ie.comment AS irradiation_event_comment,

                ge.institution_name,
                ge.institutional_department_name,
                ge.station_name,
                ge.manufacturer,
                ge.manufacturer_model_name,
                ge.device_serial_number,
                ge.software_versions,

                src.dose_rp,
                src.reference_point_definition,
                src.pulse_rate,
                src.number_of_pulses,
                src.irradiation_duration,
                src.average_xray_tube_current,
                src.exposure_time,
                src.focal_spot_size,
                src.collimated_field_area,
                src.collimated_field_height,
                src.collimated_field_width,
                src.ii_field_size,

                CASE
                    WHEN src.ii_field_size IS NOT NULL AND src.ii_field_size > 0
                    THEN ROUND(src.ii_field_size::numeric)
                    WHEN ie.comment ~ 'iiDiameter SRData="([0-9.]+)"'
                    THEN ROUND(substring(ie.comment from 'iiDiameter SRData="([0-9.]+)"')::numeric)
                    ELSE NULL
                END AS fov_mm,

                mech.positioner_primary_angle,
                mech.positioner_secondary_angle,
                mech.positioner_primary_end_angle,
                mech.positioner_secondary_end_angle,
                mech.column_angulation,
                mech.table_head_tilt_angle,
                mech.table_horizontal_rotation_angle,
                mech.table_cradle_tilt_angle,
                mech.compression_thickness,
                mech.compression_force,
                mech.magnification_factor,

                dist.distance_source_to_isocenter,
                dist.distance_source_to_reference_point,
                dist.distance_source_to_detector,
                dist.distance_source_to_entrance_surface,
                dist.table_longitudinal_position,
                dist.table_lateral_position,
                dist.table_height_position,
                dist.distance_source_to_table_plane,
                dist.table_longitudinal_end_position,
                dist.table_lateral_end_position,
                dist.table_height_end_position,
                dist.radiological_thickness,

                det.exposure_index,
                det.target_exposure_index,
                det.deviation_index,
                det.relative_xray_exposure,
                det.relative_exposure_unit,
                det.sensitivity,

                CASE
                    WHEN src.collimated_field_area IS NOT NULL
                    THEN ROUND(src.collimated_field_area::numeric * 10000.0, 4)
                    WHEN src.collimated_field_height IS NOT NULL
                     AND src.collimated_field_width IS NOT NULL
                    THEN ROUND(
                        (src.collimated_field_height::numeric * src.collimated_field_width::numeric) / 100.0,
                        4
                    )
                    ELSE NULL
                END AS field_area_cm2
            FROM remapp_irradeventxraydata ie
            JOIN remapp_projectionxrayradiationdose prd
              ON ie.projection_xray_radiation_dose_id = prd.id
            JOIN remapp_generalstudymoduleattr gs
              ON prd.general_study_module_attributes_id = gs.id
            LEFT JOIN remapp_generalequipmentmoduleattr ge
              ON ge.general_study_module_attributes_id = gs.id
            LEFT JOIN remapp_irradeventxraysourcedata src
              ON src.irradiation_event_xray_data_id = ie.id
            LEFT JOIN remapp_irradeventxraymechanicaldata mech
              ON mech.irradiation_event_xray_data_id = ie.id
            LEFT JOIN remapp_doserelateddistancemeasurements dist
              ON dist.irradiation_event_xray_mechanical_data_id = mech.id
            LEFT JOIN remapp_irradeventxraydetectordata det
              ON det.irradiation_event_xray_data_id = ie.id
            WHERE gs.study_date >= %s
              AND gs.study_date <= %s
              AND {SYSTEM_SQL_EXPR} = %s
              AND {FIRST_PHYSICIAN_SQL_EXPR} = %s
        ),
        enriched AS (
            SELECT
                *,
                CASE
                    WHEN fov_mm IS NOT NULL AND fov_mm > 0
                    THEN ROUND(fov_mm / 10.0, 4)
                    ELSE NULL
                END AS fov_side_cm,
                CASE
                    WHEN fov_mm = 480 THEN 1129.2265
                    WHEN fov_mm = 420 THEN 874.2735
                    WHEN fov_mm = 320 THEN 491.7730
                    WHEN fov_mm = 220 THEN 248.6805
                    WHEN fov_mm = 160 THEN 122.9437
                    WHEN fov_mm = 110 THEN 62.1713
                    ELSE NULL
                END AS max_fov_area_cm2,
                CASE
                    WHEN field_area_cm2 IS NOT NULL
                     AND fov_mm IN (480, 420, 320, 220, 160, 110)
                    THEN ROUND(
                        field_area_cm2::numeric /
                        (
                            CASE
                                WHEN fov_mm = 480 THEN 1129.2265
                                WHEN fov_mm = 420 THEN 874.2735
                                WHEN fov_mm = 320 THEN 491.7730
                                WHEN fov_mm = 220 THEN 248.6805
                                WHEN fov_mm = 160 THEN 122.9437
                                WHEN fov_mm = 110 THEN 62.1713
                            END
                        ) * 100.0,
                        2
                    )
                    ELSE NULL
                END AS field_usage_percent,
                CASE
                    WHEN field_area_cm2 IS NOT NULL
                     AND distance_source_to_isocenter IS NOT NULL
                     AND distance_source_to_detector IS NOT NULL
                     AND distance_source_to_detector > 0
                     AND distance_source_to_isocenter > 0
                    THEN ROUND(
                        field_area_cm2::numeric
                        * POWER(
                            distance_source_to_isocenter::numeric
                            / distance_source_to_detector::numeric,
                            2
                        ),
                        4
                    )
                    ELSE NULL
                END AS estimated_field_area_at_sod_cm2,
                CASE
                    WHEN field_area_cm2 IS NOT NULL
                     AND distance_source_to_entrance_surface IS NOT NULL
                     AND distance_source_to_detector IS NOT NULL
                     AND distance_source_to_detector > 0
                     AND distance_source_to_entrance_surface > 0
                    THEN ROUND(
                        field_area_cm2::numeric
                        * POWER(
                            distance_source_to_entrance_surface::numeric
                            / distance_source_to_detector::numeric,
                            2
                        ),
                        4
                    )
                    ELSE NULL
                END AS estimated_entrance_field_area_cm2
            FROM base
        ),
        scored AS (
            SELECT
                e.*,
                LAG(date_time_started) OVER (
                    PARTITION BY first_physician, system_name
                    ORDER BY date_time_started, irradiation_event_id
                ) AS prev_time,
                LAG(acquisition_protocol) OVER (
                    PARTITION BY first_physician, system_name
                    ORDER BY date_time_started, irradiation_event_id
                ) AS prev_protocol,
                LAG(field_area_cm2) OVER (
                    PARTITION BY first_physician, system_name
                    ORDER BY date_time_started, irradiation_event_id
                ) AS prev_field_area_cm2,
                LAG(positioner_primary_angle) OVER (
                    PARTITION BY first_physician, system_name
                    ORDER BY date_time_started, irradiation_event_id
                ) AS prev_primary_angle,
                LAG(positioner_secondary_angle) OVER (
                    PARTITION BY first_physician, system_name
                    ORDER BY date_time_started, irradiation_event_id
                ) AS prev_secondary_angle,
                LAG(fov_mm) OVER (
                    PARTITION BY first_physician, system_name
                    ORDER BY date_time_started, irradiation_event_id
                ) AS prev_fov_mm
            FROM enriched e
        ),
        final AS (
            SELECT
                *,
                (
                    CASE WHEN COALESCE(irradiation_duration, 0) >= 5 THEN 1 ELSE 0 END
                    +
                    CASE WHEN COALESCE(number_of_pulses, 0) >= 50 THEN 1 ELSE 0 END
                    +
                    CASE
                        WHEN field_usage_percent IS NOT NULL AND field_usage_percent <= 30
                        THEN 1 ELSE 0
                    END
                    +
                    CASE
                        WHEN prev_time IS NOT NULL
                         AND EXTRACT(EPOCH FROM (date_time_started - prev_time)) BETWEEN 0 AND 3
                         AND COALESCE(acquisition_protocol, '') = COALESCE(prev_protocol, '')
                         AND COALESCE(ABS(positioner_primary_angle - prev_primary_angle), 0) <= 5
                         AND COALESCE(ABS(positioner_secondary_angle - prev_secondary_angle), 0) <= 5
                         AND COALESCE(fov_mm, -1) = COALESCE(prev_fov_mm, -1)
                         AND prev_field_area_cm2 IS NOT NULL
                         AND field_area_cm2 IS NOT NULL
                         AND GREATEST(field_area_cm2, prev_field_area_cm2) > 0
                         AND ABS(field_area_cm2 - prev_field_area_cm2)
                             / GREATEST(field_area_cm2, prev_field_area_cm2) >= 0.30
                        THEN 2 ELSE 0
                    END
                ) AS collimation_change_score
            FROM scored
        )
        SELECT
            *,
            CASE
                WHEN collimation_change_score >= 4 THEN 'LIKELY'
                WHEN collimation_change_score >= 2 THEN 'REVIEW'
                ELSE ''
            END AS collimation_change_flag
        FROM final
        ORDER BY date_time_started, irradiation_event_id;
        """,
        (date_from, date_to, system_name, physician_name),
    )


def initialize_state(defaults):
    state_defaults = {
        "db_host": defaults.get("host", ""),
        "db_port": defaults.get("port", "5432"),
        "db_name": defaults.get("dbname", ""),
        "db_user": defaults.get("user", ""),
        "db_password": defaults.get("password", ""),
        "date_from": "2025-01-01",
        "date_to": "2025-12-31",
        "view_mode": "B",
        "systems": [],
        "physicians": [],
        "selected_system": None,
        "selected_physician": None,
        "events_df_all": pd.DataFrame(),
        "summary_df_all": pd.DataFrame(),
        "summary_search": "",
        "events_search": "",
        "selected_event_ids": [],
    }
    for key, value in state_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def current_config_from_state():
    return {
        "dbname": st.session_state.db_name.strip(),
        "user": st.session_state.db_user.strip(),
        "password": st.session_state.db_password,
        "host": st.session_state.db_host.strip(),
        "port": st.session_state.db_port.strip(),
    }


def config_is_complete(config):
    return all([
        config["dbname"],
        config["user"],
        config["host"],
        config["port"],
    ])


def main():
    st.set_page_config(
        page_title="OpenREM RF Physician Dose Audit",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    defaults = load_db_config()
    initialize_state(defaults)

    use_secrets = False
    try:
        use_secrets = "db" in st.secrets
    except Exception:
        use_secrets = False

    st.title("OpenREM RF Physician Dose Audit")
    st.caption(
        "Streamlit edition for OpenREM RF physician auditing. Database settings are in the sidebar. Filters live on the main page."
    )

    with st.sidebar:
        st.subheader("Database settings")
        st.caption("The sidebar starts collapsed by default. Expand it only when you need to adjust the connection.")

        if use_secrets:
            st.success("Database settings loaded from st.secrets")
            st.text_input("Host", value=st.session_state.db_host, disabled=True)
            st.text_input("Port", value=st.session_state.db_port, disabled=True)
            st.text_input("Database", value=st.session_state.db_name, disabled=True)
            st.text_input("User", value=st.session_state.db_user, disabled=True)
            st.text_input("Password", value="********", disabled=True, type="password")

            if st.button("Test connection", use_container_width=True):
                try:
                    clear_connection_cache()
                    conn = get_connection(
                        st.session_state.db_name,
                        st.session_state.db_user,
                        st.session_state.db_password,
                        st.session_state.db_host,
                        st.session_state.db_port,
                    )
                    with conn.cursor() as cur:
                        cur.execute("SELECT version();")
                        version = cur.fetchone()[0]
                    st.success(f"Connected: {version}")
                except Exception as exc:
                    st.error(str(exc))
        else:
            st.text_input("Host", key="db_host")
            st.text_input("Port", key="db_port")
            st.text_input("Database", key="db_name")
            st.text_input("User", key="db_user")
            st.text_input("Password", key="db_password", type="password")

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Save settings", use_container_width=True):
                    config = current_config_from_state()
                    save_db_config(config)
                    st.success("Connection settings saved. Password was not saved.")
            with c2:
                if st.button("Test connection", use_container_width=True):
                    try:
                        config = current_config_from_state()
                        clear_connection_cache()
                        conn = get_connection(
                            config["dbname"],
                            config["user"],
                            config["password"],
                            config["host"],
                            config["port"],
                        )
                        with conn.cursor() as cur:
                            cur.execute("SELECT version();")
                            version = cur.fetchone()[0]
                        st.success(f"Connected: {version}")
                    except Exception as exc:
                        st.error(str(exc))
            with c3:
                if st.button("Clear cache", use_container_width=True):
                    clear_connection_cache()
                    st.success("Connection cache cleared.")

    config = current_config_from_state()
    dates_valid = validate_date(st.session_state.date_from) and validate_date(st.session_state.date_to)
    date_order_valid = st.session_state.date_from <= st.session_state.date_to if dates_valid else False

    top1, top2, top3, top4, top5, top6 = st.columns([1.2, 1.2, 2.6, 2.4, 1.2, 1.2])
    with top1:
        st.text_input("Date from", key="date_from")
    with top2:
        st.text_input("Date to", key="date_to")
    with top3:
        system_index = None
        if st.session_state.selected_system in st.session_state.systems:
            system_index = st.session_state.systems.index(st.session_state.selected_system)
        st.session_state.selected_system = st.selectbox(
            "System",
            options=st.session_state.systems,
            index=system_index,
            placeholder="Select a system",
        )
    with top4:
        physician_index = None
        if st.session_state.selected_physician in st.session_state.physicians:
            physician_index = st.session_state.physicians.index(st.session_state.selected_physician)
        st.session_state.selected_physician = st.selectbox(
            "Physician",
            options=st.session_state.physicians,
            index=physician_index,
            placeholder="Select a physician",
        )
    with top5:
        st.radio("View mode", options=["A", "B", "C"], key="view_mode", horizontal=True)
    with top6:
        st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
        run_clicked = st.button("Run audit", type="primary", use_container_width=True)

    action1, action2 = st.columns(2)
    with action1:
        load_systems_clicked = st.button("Load systems", use_container_width=True)
    with action2:
        load_physicians_clicked = st.button("Load physicians", use_container_width=True)

    if not config_is_complete(config):
        st.warning("Database settings are incomplete. Open the sidebar and fill them in, or provide them through st.secrets.")

    if not dates_valid:
        st.error("Use YYYY-MM-DD for both dates.")
    elif not date_order_valid:
        st.error("Date from must be earlier than or equal to date to.")

    if load_systems_clicked:
        if not config_is_complete(config):
            st.error("Database settings are incomplete.")
        elif not dates_valid or not date_order_valid:
            st.error("Please fix the date range first.")
        else:
            try:
                clear_connection_cache()
                with st.spinner("Loading systems..."):
                    systems = get_systems(config, st.session_state.date_from, st.session_state.date_to)
                st.session_state.systems = systems
                st.session_state.selected_system = systems[0] if systems else None
                st.session_state.physicians = []
                st.session_state.selected_physician = None
                st.success(f"Loaded {len(systems)} systems.")
            except Exception as exc:
                st.error(str(exc))

    if load_physicians_clicked:
        if not config_is_complete(config):
            st.error("Database settings are incomplete.")
        elif not dates_valid or not date_order_valid:
            st.error("Please fix the date range first.")
        elif not st.session_state.selected_system:
            st.error("Select a system first.")
        else:
            try:
                clear_connection_cache()
                with st.spinner("Loading physicians..."):
                    physicians = get_physicians(
                        config,
                        st.session_state.date_from,
                        st.session_state.date_to,
                        st.session_state.selected_system,
                    )
                st.session_state.physicians = physicians
                st.session_state.selected_physician = physicians[0] if physicians else None
                st.success(f"Loaded {len(physicians)} physicians.")
            except Exception as exc:
                st.error(str(exc))

    if run_clicked:
        if not config_is_complete(config):
            st.error("Database settings are incomplete.")
        elif not dates_valid or not date_order_valid:
            st.error("Please fix the date range first.")
        elif not st.session_state.selected_system:
            st.error("Select a system.")
        elif not st.session_state.selected_physician:
            st.error("Select a physician.")
        else:
            try:
                clear_connection_cache()
                with st.spinner("Running audit..."):
                    event_headers, event_rows = run_audit_query(
                        config,
                        st.session_state.date_from,
                        st.session_state.date_to,
                        st.session_state.selected_system,
                        st.session_state.selected_physician,
                    )
                    st.session_state.events_df_all = pd.DataFrame(event_rows, columns=event_headers)
                    st.session_state.summary_df_all = build_summary_from_df(st.session_state.events_df_all)
                    st.session_state.selected_event_ids = []
                st.success(f"Loaded {len(st.session_state.events_df_all)} event rows.")
            except Exception as exc:
                st.error(str(exc))

    events_df_all = st.session_state.events_df_all.copy()
    summary_df_all = st.session_state.summary_df_all.copy()

    if events_df_all.empty:
        st.info("No results yet. Load systems, load physicians, and run the audit.")
        return

    st.divider()
    st.subheader("Quick summary")

    summary_row = summary_df_all.iloc[0].to_dict() if not summary_df_all.empty else {}
    mean_usage = summary_row.get("mean_field_usage_percent", None)
    category = collimation_category(mean_usage)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Events", summary_row.get("n_events", ""))
    m2.metric("Mean DAP (Gy·m²)", summary_row.get("mean_dap", ""))
    m3.metric("Mean dose RP (Gy)", summary_row.get("mean_dose_rp", ""))
    m4.metric("Mean FOV usage (%)", summary_row.get("mean_field_usage_percent", ""))
    m5.metric("Flagged events", summary_row.get("n_flagged_collimation_change_events", ""))

    st.write(f"**Physician:** {summary_row.get('first_physician', '')}")
    st.write(f"**System:** {summary_row.get('system_name', '')}")
    st.write(f"**Collimation category:** {category}")
    st.caption(
        "Color key: green < 50%, orange 50-79.99%, red ≥ 80%. Heuristic flags: REVIEW = score 2-3, LIKELY = score ≥ 4."
    )

    tab1, tab2 = st.tabs(["Physician summary", "Event rows"])

    with tab1:
        st.text_input("Search summary rows", key="summary_search")

        summary_headers, summary_rows = filter_columns(
            list(summary_df_all.columns),
            summary_df_all.values.tolist(),
            SUMMARY_VIEW_COLUMNS.get(st.session_state.view_mode),
        )
        summary_headers, summary_rows = remove_empty_columns(summary_headers, summary_rows)
        summary_df = pd.DataFrame(summary_rows, columns=summary_headers)
        summary_df = filter_summary_by_text(summary_df, st.session_state.summary_search)

        pretty_summary_df = to_dataframe(summary_df.columns.tolist(), summary_df.values.tolist())
        st.dataframe(pretty_summary_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download summary CSV",
            data=df_to_csv_bytes(pretty_summary_df),
            file_name=f"openrem_rf_physician_summary_view_{st.session_state.view_mode}.csv",
            mime="text/csv",
        )

    with tab2:
        st.text_input("Search accession number(s)", key="events_search")
        st.caption("Use comma, semicolon, space, or new line for multiple accession numbers.")

        event_headers, event_rows = filter_columns(
            list(events_df_all.columns),
            events_df_all.values.tolist(),
            EVENT_VIEW_COLUMNS.get(st.session_state.view_mode),
        )
        event_headers, event_rows = remove_empty_columns(event_headers, event_rows)
        visible_df = pd.DataFrame(event_rows, columns=event_headers)
        visible_df = filter_events_by_accession(visible_df, st.session_state.events_search)

        if "irradiation_event_id" in visible_df.columns:
            visible_ids = visible_df["irradiation_event_id"].tolist()
            existing_selected = [x for x in st.session_state.selected_event_ids if x in visible_ids]
            st.session_state.selected_event_ids = st.multiselect(
                "Optional: build summary from selected visible event IDs",
                options=visible_ids,
                default=existing_selected,
            )

        basis_df = visible_df.copy()
        if st.session_state.selected_event_ids and "irradiation_event_id" in visible_df.columns:
            basis_df = visible_df[visible_df["irradiation_event_id"].isin(st.session_state.selected_event_ids)].copy()
            st.info(f"Summary basis: {len(basis_df)} selected visible rows")
        else:
            st.info(f"Summary basis: all visible rows ({len(basis_df)})")

        rebuilt_summary_df = build_summary_from_df(basis_df)
        if not rebuilt_summary_df.empty:
            st.markdown("**Selection-based summary preview**")
            st.dataframe(
                to_dataframe(rebuilt_summary_df.columns.tolist(), rebuilt_summary_df.values.tolist()),
                use_container_width=True,
                hide_index=True,
            )

        pretty_events_df = to_dataframe(visible_df.columns.tolist(), visible_df.values.tolist())
        st.dataframe(style_events(pretty_events_df), use_container_width=True, hide_index=True)
        st.download_button(
            "Download event rows CSV",
            data=df_to_csv_bytes(pretty_events_df),
            file_name=f"openrem_rf_event_rows_view_{st.session_state.view_mode}.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()

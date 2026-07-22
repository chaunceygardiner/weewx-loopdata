#!/usr/bin/env python3
# Spec-only test runner for mutation testing.  Includes ONLY tests whose
# expected values were derived independently from spec/first principles --
# excludes the seven packet-file regression tests AND all gray-zone tests whose
# expected values may have been frozen from loopdata output.
import sys, unittest
import test_process_packet as T

SPEC_TESTS = [
    'test_parse_cname',
    'test_compose_loop_data_dir',
    'test_period_classification',
    'test_get_windrun_bucket',
    'test_massage_near_zero',
    'test_construct_baro_trend_descs',
    'test_compute_period_obstypes',
    'test_compute_period_obstypes_isolated_composites',
    'test_get_fields_to_include',
    'test_parse_almanac_field',
    'test_get_almanac_fields',
    'test_almanac_field_evaluator',
    'test_almanac_field_end_to_end',
    'test_get_barometer_trend_mbar',
    'test_get_barometer_trend_inHg',
    'test_get_trend_computation',
    'test_add_trend_obstype_barometer_code_desc',
    'test_prune_period_packet',
    'test_day_wind_vecdir_vecavg',
    'test_continuous_wind_vecdir_expiry',
    'test_min_max_dict',
    'test_continuous_scalar_stats_edge_cases',
    'test_continuous_vec_stats_edge_cases',
    'test_continuous_vec_stats_trim_debits_every_field',
    'test_continuous_vec_stats_trim_boundary_and_calm',
    'test_continuous_vec_stats_getstatstuple_and_accessors',
    'test_continuous_vec_stats_addsum_credits',
    'test_continuous_scalar_stats_sums_and_trim',
    'test_add_period_obstype_scalar_agg_dispatch',
    'test_agg_type_grammar_is_dispatch_union',
    'test_agg_extractors_reference_real_slots',
    'test_add_current_obstype_format_spec_dispatch',
    'test_create_loopdata_packet_period_routing',
    'test_unit_override_current',
    'test_unit_override_period',
    'test_time_context_formatting',
    'test_unit_override_trend_offset_unit',
    'test_add_period_obstype_vec_agg_dispatch',
    'test_continuous_firstlast_accum_basic',
    'test_continuous_accum_units_and_wind',
    'test_continuous_vec_ysum_separation',
    'test_continuous_vec_dir_wraparound_and_zero_vector',
    'test_get_trend_guard_branches',
    'test_day_wind_vecdir_loop_vs_quantized_archive',
    'test_create_period_accum_from_database',
    'test_create_period_accum_empty_obstypes',
    'test_create_period_accum_day_summary_upper_bound',
    'test_continuous_firstlast_accum_empty',
    'test_firstlast_obstype_end_to_end',
    'test_period_accum_wrappers_distinct_spans',
    'test_period_accum_wrappers_use_correct_spans',
    'test_create_hour_accum_from_database',
    'test_create_continuous_accum_from_database',
]

suite = unittest.TestSuite([T.ProcessPacketTests(name) for name in SPEC_TESTS])
result = unittest.TextTestRunner(verbosity=0).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)

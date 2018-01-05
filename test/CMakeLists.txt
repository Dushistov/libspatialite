set(check_PROGRAMS check_endian
  check_version
  check_init
  check_init2
  check_init_full
  check_geom_aux
  check_geometry_cols
  check_create
  check_bufovflw
  check_fdo1
  check_fdo2
  check_fdo3
  check_fdo_bufovflw
  check_md5
  check_dbf_load
  check_shp_load
  check_shp_load_3d
  shape_cp1252
  shape_primitives
  shape_utf8_1
  shape_utf8_1ex
  shape_utf8_2
  shape_3d
  check_clone_table
  check_xls_load
  check_math_funcs
  check_gaia_util
  check_gaia_utf8
  check_extension
  check_recover_geom
  check_sql_stmt
  check_multithread
  check_virtualtable1
  check_virtualtable2
  check_virtualtable3
  check_virtualtable4
  check_virtualtable5
  check_virtualtable6
  check_virtual_ovflw
  check_mbrcache
  check_spatialindex
  check_exif
  check_exif2
  check_relations_fncts
  check_extra_relations_fncts
  check_geoscvt_fncts
  check_libxml2
  check_styling
  check_virtualxpath
  check_virtualbbox
  check_wfsin
  check_dxf
  check_metacatalog
  check_virtualelem
  check_srid_fncts
  check_control_points
  check_cutter
  check_topology2d
  check_topology3d
  check_toponoface2d
  check_topoplus
  check_toposnap
  check_network2d
  check_network3d
  check_network_log
  check_virtualknn
  check_sequence
  check_stored_proc
  check_wms)

if (ENABLE_GEOPACKAGE)
  set(geopackage_check_PROGRAMS
    check_createBaseTables
    check_gpkgCreateTilesTable
    check_gpkgCreateTilesTableMissingSRID
    check_gpkgCreateTilesZoomLevel
    check_gpkgInsertEpsgSRID check_gpkgMode
    check_gpkgCreateFeaturesTable
    check_gpkg_base_core_container_data_file_format_application_id
    check_gpkg_base_core_spatial_ref_sys_data_table_def
    check_gpkg_base_core_spatial_ref_sys_data_values_default
    check_gpkg_base_core_contents_data_table_def
    check_gpkg_opt_extension_mechanism_extensions_data_table_def
    check_add_tile_triggers
    check_add_tile_triggers_bad_table_name
    check_get_normal_row
    check_get_normal_row_bad_geopackage
    check_get_normal_row_bad_geopackage2
    check_get_normal_zoom
    check_get_normal_zoom_extension_load
    check_get_normal_zoom_bad_geopackage
    check_get_normal_zoom_bad_geopackage2
    check_gpkgGetImageFormat
    check_gpkgGetImageFormat_nonimage
    check_gpkgGetImageFormat_nonblob
    check_gpkgGetImageFormat_png
    check_gpkgGetImageFormat_tiff
    check_gpkgGetImageFormat_webp
    check_gpkgConvert
    check_gpkgVirtual)
  set(check_PROGRAMS ${check_PROGRAMS} ${geopackage_check_PROGRAMS})
endif (ENABLE_GEOPACKAGE)

foreach(test ${check_PROGRAMS})
  add_executable(${test} "${test}.c")
  target_link_libraries(${test} spatialite "-lm" "-lpthread" "-lz")
  add_test(NAME ${test} COMMAND ${test} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endforeach ()

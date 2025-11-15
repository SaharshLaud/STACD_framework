import ee
# GEO server imports removed for REST API
from computing.utils.gee_utils import (
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
)


def vectorise_lulc( state, district, block, start_year, end_year):
    ee.Initialize(project='ee-saharshlaud')


    fc = ee.FeatureCollection(
        get_gee_asset_path(state, district, block, asset_path="projects/ee-corestackdev/assets/apps/mws/")
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    
    lulc_list = []
    s_year = start_year  # START_YEAR
    while s_year <= end_year:
        lulc_list.append(
            ee.Image(
                get_gee_asset_path(state, district, block, "projects/ee-saharshlaud/assets/apps/mws/")
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + str(s_year)
                + "-07-01_"
                + str(s_year + 1)
                + "-06-30_LULCmap_10m"
            )
        )
        s_year += 1
    
    lulc = ee.List(lulc_list)

    # 0 - Background
    # 1 - Built-up
    # 2 - Water in Kharif
    # 3 - Water in Kharif+Rabi
    # 4 - Water in Kharif+Rabi+Zaid
    # 6 - Tree/Forests
    # 7 - Barrenlands
    # 8 - Single cropping cropland
    # 9 - Single Non-Kharif cropping cropland
    # 10 - Double cropping cropland
    # 11 - Triple cropping cropland
    # 12 - Shrub_Scrub

    args = [
        {"label": 1, "txt": "built-up_area_"},
        {"label": 2, "txt": "k_water_area_"},
        {"label": 3, "txt": "kr_water_area_"},
        {"label": 4, "txt": "krz_water_area_"},
        {"label": 5, "txt": "cropland_area_"},
        {"label": 6, "txt": "tree_forest_area_"},
        {"label": 7, "txt": "barrenlands_area_"},
        {"label": 8, "txt": "single_kharif_cropped_area_"},
        {"label": 9, "txt": "single_non_kharif_cropped_area_"},
        {"label": 10, "txt": "doubly_cropped_area_"},
        {"label": 11, "txt": "triply_cropped_area_"},
        {"label": 12, "txt": "shrub_scrub_area_"},
    ]

    

    def res(feature):
        value = feature.get("sum")
        value = ee.Number(value).divide(10000)
        return feature.set(arg["txt"] + str(sy), value)
    
    
    for arg in args:
        s_year = start_year
        while s_year <= end_year:
            sy = s_year
            image = ee.Image(lulc.get(sy - start_year)).select(["predicted_label"])
            mask = image.eq(ee.Number(arg["label"]))
            pixel_area = ee.Image.pixelArea()
            forest_area = pixel_area.updateMask(mask)
            fc = forest_area.reduceRegions(fc, ee.Reducer.sum(), 10, image.projection())
            s_year += 1
            fc = fc.map(res)

    
    fc = ee.FeatureCollection(fc)
    

    description = (
        "lulc_vector_" + valid_gee_text(district) + "_" + valid_gee_text(block)
    )

    task = ee.batch.Export.table.toAsset(
        **{
            "collection": fc,
            "description": description,
            "assetId": get_gee_asset_path(state, district, block, "projects/ee-saharshlaud/assets/apps/mws/") + description,
        }
    )
    
    task.start()    

    task_status = check_task_status([task.status()["id"]])
    print("Task completed - ", task_status)

        # Build asset ID
    asset_id = get_gee_asset_path(state, district, block, "projects/ee-saharshlaud/assets/apps/mws/") + description

    fc = ee.FeatureCollection(asset_id).getInfo()
    
    # Return asset ID for Airflow to use
    return asset_id

    # fc = {"features": fc["features"], "type": fc["type"]}
    # res = # sync_layer_to_geoserver(
    #     state,
    #     fc,
    #     "lulc_vector_"
    #     + valid_gee_text(district.lower())
    #     + "_"
    #     + valid_gee_text(block.lower()),
    #     "lulc_vector",
    # )
    # print(res)

# def vectorise_lulc( state, district, block, start_year, end_year):
#     fc = ee.FeatureCollection(
#         get_gee_asset_path(state, district, block, asset_path="projects/ee-corestackdev/assets/apps/mws/")
#         + "filtered_mws_"
#         + valid_gee_text(district.lower())
#         + "_"
#         + valid_gee_text(block.lower())
#         + "_uid"
#     )

#     lulc_list = []
#     s_year = start_year  # START_YEAR
#     while s_year <= end_year:
#         lulc_list.append(
#             ee.Image(
#                 get_gee_asset_path(state, district, block, asset_path="projects/ee-corestackdev/assets/apps/mws/")
#                 + valid_gee_text(district.lower())
#                 + "_"
#                 + valid_gee_text(block.lower())
#                 + "_"
#                 + str(s_year)
#                 + "-07-01_"
#                 + str(s_year + 1)
#                 + "-06-30_LULCmap_10m"
#             )
#         )
#         s_year += 1

#     lulc = ee.List(lulc_list)

#     # 0 - Background
#     # 1 - Built-up
#     # 2 - Water in Kharif
#     # 3 - Water in Kharif+Rabi
#     # 4 - Water in Kharif+Rabi+Zaid
#     # 6 - Tree/Forests
#     # 7 - Barrenlands
#     # 8 - Single cropping cropland
#     # 9 - Single Non-Kharif cropping cropland
#     # 10 - Double cropping cropland
#     # 11 - Triple cropping cropland
#     # 12 - Shrub_Scrub

#     args = [
#         {"label": 1, "txt": "built-up_area_"},
#         {"label": 2, "txt": "k_water_area_"},
#         {"label": 3, "txt": "kr_water_area_"},
#         {"label": 4, "txt": "krz_water_area_"},
#         {"label": 5, "txt": "cropland_area_"},
#         {"label": 6, "txt": "tree_forest_area_"},
#         {"label": 7, "txt": "barrenlands_area_"},
#         {"label": 8, "txt": "single_kharif_cropped_area_"},
#         {"label": 9, "txt": "single_non_kharif_cropped_area_"},
#         {"label": 10, "txt": "doubly_cropped_area_"},
#         {"label": 11, "txt": "triply_cropped_area_"},
#         {"label": 12, "txt": "shrub_scrub_area_"},
#     ]

#     def res(feature):
#         value = feature.get("sum")
#         value = ee.Number(value).divide(10000)
#         return feature.set(arg["txt"] + str(sy), value)

#     for arg in args:
#         s_year = start_year
#         while s_year <= end_year:
#             sy = s_year
#             image = ee.Image(lulc.get(sy - start_year)).select(["predicted_label"])
#             mask = image.eq(ee.Number(arg["label"]))
#             pixel_area = ee.Image.pixelArea()
#             forest_area = pixel_area.updateMask(mask)
#             fc = forest_area.reduceRegions(fc, ee.Reducer.sum(), 10, image.projection())
#             s_year += 1
#             fc = fc.map(res)

#     fc = ee.FeatureCollection(fc)

#     description = (
#         "lulc_vector_" + valid_gee_text(district) + "_" + valid_gee_text(block)
#     )

#     task = ee.batch.Export.table.toAsset(
#         **{
#             "collection": fc,
#             "description": description,
#             "assetId": get_gee_asset_path(state, district, block, asset_path="projects/ee-corestackdev/assets/apps/mws/") + description,
#         }
#     )
#     task.start()

#     task_status = check_task_status([task.status()["id"]])
#     print("Task completed - ", task_status)

#     fc = ee.FeatureCollection(
#         get_gee_asset_path(state, district, block, asset_path="projects/ee-corestackdev/assets/apps/mws/") + description
#     ).getInfo()

#     fc = {"features": fc["features"], "type": fc["type"]}
#     res = # sync_layer_to_geoserver(
#         state,
#         fc,
#         "lulc_vector_"
#         + valid_gee_text(district.lower())
#         + "_"
#         + valid_gee_text(block.lower()),
#         "lulc_vector",
#     )
#     print(res)

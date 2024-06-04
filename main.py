import os
import sys
import ast
import warnings

import json
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text
from fiona.drvsupport import supported_drivers
import subprocess as sub

from sqlalchemy.orm import sessionmaker
from datetime import datetime

warnings.filterwarnings('ignore')

supported_drivers['KML'] = 'rw'
supported_drivers['sqlite'] = 'rw'
supported_drivers['LIBKML'] = 'rw'


def query_footprint(state_abbr, fips, provider_id, tech_code, max_download, max_upload, br_code):
    query = f"""
        SELECT * FROM
        ww_get_all_cb_polygons_20(ARRAY[{state_abbr}], 
        ARRAY[{fips}], 
        ARRAY[{provider_id}], ARRAY[{tech_code}], 
        ARRAY[{max_download}], ARRAY[{max_upload}], 
        ARRAY[{br_code}]);
    """
    print(query)
    return query


def query_locations(state_abbr, fips):
    query = f"""
SELECT fcc.*                                 ,
       stags.categories_served_unserved      ,
       stags.categories_cd_ucd               ,
       stags.categories_mso                  ,
       stags.categories_cafii                ,
       stags.categories_rdof                 ,
       stags.categories_other_federal_grants ,
       stags.categories_unserved_and_unfunded,
       stags.categories_high_cost            ,
       uc.county_name                        ,
       tgs.wired_dl25_ul3_r                  ,
       tgs.wired_dl100_ul20_r                ,
       tgs.terrestrial_dl25_ul3_r            ,
       tgs.terrestrial_dl100_ul20_r          ,
       tgs.wiredlfw_dl25_ul3_r               ,
       tgs.wiredlfw_dl100_ul20_r             ,
       tgs.wired_dl25_ul3_b                  ,
       tgs.wired_dl100_ul20_b                ,
       tgs.terrestrial_dl25_ul3_b            ,
       tgs.terrestrial_dl100_ul20_b          ,
       tgs.wiredlfw_dl25_ul3_b               ,
       tgs.wiredlfw_dl100_ul20_b
    FROM us_sw2020_fabric_harvested_rel4_full fcc
        INNER JOIN fcc_bdc_fabric_rel4 stags ON fcc.fcc_location_id = stags.fcc_location_id
        LEFT JOIN us_sw2020_fabric_harvested_new_taggs tgs on fcc.fcc_location_id = tgs.location_id
        INNER JOIN us_counties uc ON fcc.fips_2020 = uc.fips_code
    WHERE fcc.state_abbr = ANY(ARRAY[{state_abbr}]) AND fcc.fips_2020 = ANY(ARRAY[{fips}])
    AND tgs.wiredlfw_dl25_ul3_r = 'U'
    AND tgs.wiredlfw_dl25_ul3_b = 'U'
    AND tgs.wiredlfw_dl100_ul20_r = 'U'
    AND tgs.wiredlfw_dl100_ul20_b = 'U'
    """
    print(query)
    return query


# def get_filtered_fips(provider_id, state, con):
#     query = f"""
#     SELECT DISTINCT fips_code
#     FROM us_census_block_data
#     WHERE provider_id = ANY(%s) AND state_abbr = ANY(%s)
#     """
#     print(query)
#     return pd.read_sql(query, con, params=(provider_id, state))
#
#
# def create_temp_table(con, fips_codes):
#     fips_codes.to_sql('temp_fips_codes', con, index=False, if_exists='replace', schema='public')
#

def query_counties_by_provider(provider_id, state, table_name):
    query = f"""
    SELECT uc.* 
    FROM us_counties uc
    INNER JOIN {table_name} temp ON temp.county_fips = uc.fips 
    """
    # WHERE fips IN
    #     (SELECT
    #         DISTINCT fips_code
    #      FROM us_census_block_data cb
    #      WHERE cb.provider_id = ANY(ARRAY[{provider_id}]) AND cb.state_abbr = ANY(ARRAY[{state}]))
    print(query)
    return query


def get_federal_grants(provider_id, state_abbr, con, table_name):
    query = f"""
SELECT info.*, gm.geometry FROM
(
    SELECT gt.id,
       ag.agency_name,
       ag.funding_program_name,
       ag.program_id,
       gt.project_id,
       gt.project,
       gt.brandname,
       gt.providerid,
       gt.build_req,
       gt.loc_plan,
       gt.loc_sup,
       gt.technology_code,
       gt.technology_name,
       gt.maxdown,
       gt.maxup,
       uc.state_abbr,
       uc.county_name,
       uc.fips,
       gt.source,
       gt.source_dat,
       gt.categories_served
    FROM us_federal_grants gt
    INNER JOIN agencies ag ON gt.program_id = ag.program_id
    INNER JOIN federal_gt_counties_pivot pivot on gt.id = pivot.grant_id
    INNER JOIN us_counties uc on pivot.county_id = uc.id
    INNER JOIN {table_name} temp ON uc.fips = temp.county_fips
    WHERE uc.state_abbr = ANY(ARRAY[{state_abbr}])) info
INNER JOIN us_federal_grants_geometry gm ON info.id = gm.id;
    """
    print(query)
    gdf = gpd.read_postgis(query, con, geom_col='geometry', crs='ESRI:102008')
    return gdf


def get_hex(provider_id, state, con, table_name):
    query = f"""
    SELECT h3.*
    FROM us_fcc_joined_h3_resolution8_test h3
    INNER JOIN {table_name} temp ON h3.county_fips = temp.county_fips
    """
    print(query)
    # WHERE
    # county_fips
    # IN(SELECT
    # DISTINCT
    # fips_code
    # FROM
    # us_census_block_data
    # cb
    # WHERE
    # cb.provider_id = ANY(ARRAY[{provider_id}])
    # AND
    # cb.state_abbr = ANY(ARRAY[{state}]));
    hex = gpd.GeoDataFrame.from_postgis(query, con, geom_col='geom', crs='EPSG:4326').to_crs('ESRI:102008')

    return hex


def get_fip_codes(polygon_data, con, state_abbr):
    query = f"""
        SELECT state_abbr,fips,county_name
        FROM us_counties
        WHERE ST_Intersects(
            geom,
            ST_GeomFromText('{polygon_data.geometry.iloc[0]}', 102008)) AND state_abbr = ANY(ARRAY[{state_abbr}])
    """
    counties = pd.read_sql(query, con)
    print(query)
    return list(counties["fips"].unique())


def query_state(state_abbr):
    query = f"""
        SELECT * FROM "USStates" WHERE "StateAbbr" = ANY(ARRAY[{state_abbr}])
    """
    print(query)
    return query


def query_counties(fips):
    query = f"""
        SELECT * FROM us_counties WHERE fips = ANY(ARRAY[{fips}])
    """
    print(query)
    return query


def write_gradient_ranges_staticly(gdf, path=r'C:\OSGeo4W\processing_utilities'):
    range_dict = {
        "0": {"range": (1, 3), "color": "#e4e4f3"},
        "1": {"range": (3, 5), "color": "#dbdbee"},
        "2": {"range": (5, 7), "color": "#d1d1ea"},
        "3": {"range": (7, 10), "color": "#c8c8e6"},
        "4": {"range": (10, 15), "color": "#adadda"},
        "5": {"range": (15, 20), "color": "#9b9bd1"},
        "6": {"range": (20, 25), "color": "#8080c5"},
        "7": {"range": (25, 30), "color": "#6d6dbd"},
        "8": {"range": (30, 40), "color": "#5252b0"},
        "9": {"range": (40, 50), "color": "#4949ac"},
        "10": {"range": (50, 75), "color": "#4040a8"},
        "11": {"range": (75, 100), "color": "#3737a4"},
        "12": {"range": (100, 150), "color": "#2e2ea0"},
        "13": {"range": (150, 200), "color": "#24249c"},
        "14": {"range": (200, 300), "color": "#1b1b97"},
        "15": {"range": (300, 400), "color": "#121293"},
        "16": {"range": (400, 500), "color": "#09098f"},
        "17": {"range": (500, 50000), "color": "#00008b"}
    }

    max_number = gdf["Unserved_Unfunded"].unique().max()

    new_dict = dict()
    for key, value in range_dict.items():
        if max_number >= range_dict[key]["range"][0]:
            new_dict[key] = value
        else:
            break
    save_path = path + '/dict.txt'

    with open(save_path, 'w') as convert_file:
        convert_file.write(json.dumps(new_dict))

    return save_path


def mile_to_meter(miles):
    return miles * 1609.34


def create_formatted_excel(provider_name, market, unserved_unfunded_FP,
                           unserved_unfunded_10_miles, unserved_unfunded_30_miles,
                           in_footprint_counties, file_path, locations_within_counties, location_in_fp):
    df = pd.DataFrame({
        'Provider Name': [provider_name],
        'Market': [market],
        'Number of Unserved & Unfunded Locations in FP': [unserved_unfunded_FP],
        'Number of Unserved & Unfunded Locations in 10 Miles Buffer Ring': [unserved_unfunded_10_miles],
        'Number of Unserved & Unfunded Locations in 30 Miles Buffer Ring': [unserved_unfunded_30_miles],
        'In Footprint Counties': [in_footprint_counties]
    })

    # Start a writer instance using xlsxwriter
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')

    # Access the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # Define the formats
    header_format = workbook.add_format({'bg_color': '#9BBB59', 'bold': True})
    header_format2 = workbook.add_format({'bg_color': '#F79646', 'bold': True})
    provider_format = workbook.add_format({'bg_color': '#C5D9F1', 'bold': True})
    bold_format = workbook.add_format({'bold': True})
    light_blue_format = workbook.add_format({'bg_color': '#DAE8FC', 'bold': True})

    # Apply the formats to the header cells
    worksheet.write('A1', 'Provider Name', header_format)
    worksheet.write('B1', 'Market', header_format)
    worksheet.write('C1', 'Number of Unserved & Unfunded Locations in FP', header_format2)
    worksheet.write('D1', 'Number of Unserved & Unfunded Locations in 10 Miles Buffer Ring', header_format2)
    worksheet.write('E1', 'Number of Unserved & Unfunded Locations in 30 Miles Buffer Ring', header_format2)
    worksheet.write('F1', 'Number of Unserved & Unfunded Locations in Counties of FP', light_blue_format)  # New header

    # Apply bold format to 'Provider Name' and 'Market' values
    worksheet.write('A2', provider_name, provider_format)
    worksheet.write('B2', market, bold_format)
    worksheet.write('F2', in_footprint_counties, light_blue_format)  # New value

    # Set the column widths
    worksheet.set_column('A:A', 20)
    worksheet.set_column('B:B', 10)
    worksheet.set_column('C:C', 35)
    worksheet.set_column('D:E', 45)
    worksheet.set_column('F:F', 20)  # New column width

    # Calculate counts and merge dataframes
    locations_within_counties_counts = locations_within_counties['county_name'].value_counts().reset_index()
    locations_within_counties_counts.columns = ['County', 'Count of Locations']

    location_in_fp_counts = location_in_fp['county_name'].value_counts().reset_index()
    location_in_fp_counts.columns = ['County', 'Count of Locations in FP']

    high_cost_counts = locations_within_counties[locations_within_counties['categories_high_cost'] == True][
        'county_name'].value_counts().reset_index()
    high_cost_counts.columns = ['County', 'Count of High Cost']

    merged_df = locations_within_counties_counts.merge(location_in_fp_counts, on='County', how='left').merge(
        high_cost_counts, on='County', how='left')
    merged_df['Count of Locations in FP'] = merged_df['Count of Locations in FP'].fillna(0).astype(int)
    merged_df['Count of High Cost'] = merged_df['Count of High Cost'].fillna(0).astype(int)

    # Write the new DataFrame to a new sheet in the same Excel file
    merged_df.to_excel(writer, index=False, sheet_name='County Counts and High Cost')

    # Access the new worksheet
    merged_worksheet = writer.sheets['County Counts and High Cost']

    # Apply header format to the new worksheet
    merged_worksheet.write('A1', 'County Name', header_format)
    merged_worksheet.write('B1', 'Number of Unserved & Unfunded Locations in County', header_format)
    merged_worksheet.write('C1', 'Number of Unserved & Unfunded Locations in FP', header_format)
    merged_worksheet.write('D1', 'High Cost', header_format)

    # Set the column widths for the new worksheet
    merged_worksheet.set_column('A:A', 15)
    merged_worksheet.set_column('B:B', 20)
    merged_worksheet.set_column('C:C', 25)
    merged_worksheet.set_column('D:D', 20)

    # Close the Pandas Excel writer and output the Excel file
    writer.close()


def call_qgis_for_30_10(name_of_project, state_name, unserved_unfunded_in_fp, unserved_unfunded_10,
                        unserved_unfunded_30, state_polygon, county_polygon, cb_footprint,
                        cb_footprint_10, cb_footprint_30, provider_name, project_path, hex_layer, counties_footprint,
                        locations_in_counties, grad_path, ntia_path, rus_path, fcc_path, usac_path, treasury_path):
    my_call = [r"C:\OSGeo4W\OSGeo4W.bat", r"python-qgis",
               r"C:\OSGeo4W\processing_utilities\save_30_10_buffer.py",
               name_of_project, state_name, unserved_unfunded_in_fp, unserved_unfunded_10,
               unserved_unfunded_30, state_polygon, county_polygon, cb_footprint,
               cb_footprint_10, cb_footprint_30, provider_name, project_path, hex_layer, counties_footprint,
               locations_in_counties, grad_path, ntia_path, rus_path, fcc_path, usac_path, treasury_path]
    p = sub.Popen(my_call, stdout=sub.PIPE, stderr=sub.PIPE)
    stdout, stderr = p.communicate()

    print(stdout, stderr)
    return stdout, stderr


def main():
    # state_abbr = ['AR']
    # fips = ['29043', '05141', '05071', '29067', '05019', '05001', '05065', '05009', '05135']
    # provider_id = [290111]
    # tech_code = [60]
    # max_download = [-1]
    # max_upload = [-1]
    # br_code = [-1]
    # provider_name = "sample Prov"
    # path = r"C:\Users\meloy\PycharmProjects\Wireless2020\30-10MileBuffer"
    # state_name = "Arkansaas"
    # file_type = 'sqlite'

    state_abbr = sys.argv[1]
    fips = sys.argv[2]
    provider_id = sys.argv[3]
    tech_code = sys.argv[4]
    max_download = sys.argv[5]
    max_upload = sys.argv[6]
    br_code = sys.argv[7]
    provider_name = sys.argv[8]
    path = sys.argv[9]
    state_name = sys.argv[10]
    file_type = sys.argv[11]

    os.mkdir(os.path.join(path, "results"))
    save_path = os.path.join(path, "results")

    db_connection_url = "postgresql://postgresqlwireless2020:software2020!!@wirelesspostgresqlflexible.postgres.database.azure.com:5432/wiroidb2"
    con = create_engine(db_connection_url)
    try:
        metadata = MetaData()

        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = f"temp_fips_{current_timestamp}"

        temp_fips_table = Table(
            table_name, metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('county_fips', String(255))
        )

        metadata.create_all(con)

        Session = sessionmaker(bind=con)
        session = Session()

        insert_data_query = f"""
        INSERT INTO {table_name} (county_fips)
        SELECT DISTINCT fips_code
        FROM us_census_block_data cb
        WHERE provider_id = ANY(ARRAY[{provider_id}]) AND cb.state_abbr = ANY(ARRAY[{state_abbr}]);
        """

        session.execute(
            text(insert_data_query))
        session.commit()

        query_fp = query_footprint(state_abbr, fips, provider_id, tech_code, max_download, max_upload, br_code)
        footprint_raw = gpd.GeoDataFrame.from_postgis(query_fp, con, geom_col='Geometry',
                                                      crs='EPSG:4326').to_crs("ESRI:102008")
        footprint = footprint_raw.dissolve()
        ten_mile_buffer = footprint.buffer(mile_to_meter(10))
        ten_mile_dif = ten_mile_buffer.difference(footprint)
        thirty_mile_dif_raw = footprint.buffer(mile_to_meter(30))
        thirty_mile_dif = thirty_mile_dif_raw.difference(ten_mile_buffer)

        fips = get_fip_codes(thirty_mile_dif_raw, con, state_abbr)

        # fips_fp = get_fip_codes(footprint, con, state_abbr)

        query_loc = query_locations(state_abbr, fips)
        locations = gpd.GeoDataFrame.from_postgis(query_loc, con, geom_col='geom', crs='EPSG:4326').to_crs(
            "ESRI:102008")

        locations = locations.drop_duplicates(subset=['fcc_location_id'], keep='last')

        state = gpd.GeoDataFrame.from_postgis(query_state(state_abbr), con, geom_col='geometry',
                                              crs='ESRI:102008').to_crs('EPSG:4326')

        counties = gpd.GeoDataFrame.from_postgis(query_counties(fips), con, geom_col='geom', crs='ESRI:102008').to_crs(
            'EPSG:4326')

        counties_fp = gpd.GeoDataFrame.from_postgis(query_counties_by_provider(provider_id, state_abbr, table_name), con,
                                                    geom_col='geom',
                                                    crs='ESRI:102008').to_crs(
            'EPSG:4326')

        locations_in_cb_footprint = gpd.sjoin(locations, footprint_raw, how="inner", op='intersects').to_crs(
            'EPSG:4326')
        locations_in_cb_footprint = locations_in_cb_footprint[locations.columns].drop_duplicates(
            subset='fcc_location_id', keep='last')

        locations_in_10mileBuffer = gpd.sjoin(locations, gpd.GeoDataFrame(geometry=ten_mile_dif), how="inner",
                                              op='intersects').to_crs('EPSG:4326')
        locations_in_10mileBuffer = locations_in_10mileBuffer[locations.columns].drop_duplicates(
            subset='fcc_location_id', keep='last')

        locations_in_30mileBuffer = gpd.sjoin(locations, gpd.GeoDataFrame(geometry=thirty_mile_dif), how="inner",
                                              op='intersects').to_crs('EPSG:4326')
        locations_in_30mileBuffer = locations_in_30mileBuffer[locations.columns].drop_duplicates(
            subset='fcc_location_id', keep='last')

        ten_mile_dif = ten_mile_dif.to_crs('EPSG:4326')
        thirty_mile_dif = thirty_mile_dif.to_crs('EPSG:4326')
        footprint = footprint.to_crs('EPSG:4326')
        ten_mile_dif = gpd.GeoDataFrame(geometry=ten_mile_dif)
        thirty_mile_dif = gpd.GeoDataFrame(geometry=thirty_mile_dif)

        ten_mile_dif = gpd.overlay(ten_mile_dif, state, how='intersection')

        thirty_mile_dif = gpd.overlay(thirty_mile_dif, state, how='intersection')

        hex_gdf = get_hex(provider_id, state_abbr, con, table_name=table_name)

        joined_gdf = gpd.sjoin(hex_gdf, locations, how="inner", op='contains')

        location_counts = joined_gdf.groupby(joined_gdf.index).size()

        hex_gdf['Unserved_Unfunded'] = \
            hex_gdf.merge(location_counts.rename('Unserved_Unfunded'), how='left', left_index=True, right_index=True)[
                'Unserved_Unfunded']

        hex_gdf = hex_gdf.dropna(subset=['Unserved_Unfunded'])

        hex_gdf['Unserved_Unfunded'] = hex_gdf['Unserved_Unfunded'].fillna(0)

        hex_gdf = hex_gdf[hex_gdf['Unserved_Unfunded'] > 0]

        hex_gdf = hex_gdf.to_crs('EPSG:4326')
        hex_gdf = hex_gdf.drop_duplicates(subset='h3_res8_id', keep='last')

        hex_gdf = hex_gdf.drop(
            columns=['frn', 'provider_id', 'brand_name', 'Technology Code', 'max_advertised_download_speed',
                     'max_advertised_upload_speed', 'low_latency', 'br_code', 'max_down_id', 'max_up_id', 'id',
                     'technology', 'state_abbr'])
        gradient_path = write_gradient_ranges_staticly(hex_gdf)

        locations_within_counties = gpd.sjoin(locations,
                                              counties_fp.to_crs('ESRI:102008').drop(
                                                  columns=['id', 'state_abbr', 'county_name']),
                                              how="inner",
                                              op='within').to_crs('EPSG:4326')
        locations_within_counties = locations_within_counties[locations.columns].drop_duplicates(
            subset='fcc_location_id', keep='last')

        federal_grants = get_federal_grants(provider_id, state_abbr, con, table_name=table_name).to_crs('EPSG:4326')

        clipped_geometries = []
        counties_fl = counties_fp.dissolve()
        for idx, row in federal_grants.iterrows():
            clipped_geometry = row['geometry'].intersection(counties_fl.unary_union)
            if not clipped_geometry.is_empty:
                new_row = row.copy()
                new_row['geometry'] = clipped_geometry
                clipped_geometries.append(new_row)

        federal_grants = gpd.GeoDataFrame(clipped_geometries, crs=federal_grants.crs, geometry='geometry')

        extension = '.shp'
        if file_type == 'sqlite':
            extension = '.sqlite'
        elif file_type == 'kml':
            extension = '.kml'

        export_type_dict = {".shp": ['ESRI Shapefile'],
                            '.sqlite': ['sqlite'], '.kml': ["KML"]}

        cb_footprint = os.path.join(save_path, f'{provider_name} CB Footprint{extension}')
        cb_footprint_10 = os.path.join(save_path, f'{provider_name} CB Footprint 10 Miles Buffer Ring{extension}')
        cb_footprint_30 = os.path.join(save_path, f'{provider_name} CB Footprint 30 Miles Buffer Ring{extension}')
        unserved_unfunded_in_fp = os.path.join(save_path, f'Unserved and Unfunded Locations in FP{extension}')
        unserved_unfunded_10 = os.path.join(save_path,
                                            f'Unserved and Unfunded Locations in 10 Miles Buffer Ring{extension}')
        unserved_unfunded_30 = os.path.join(save_path,
                                            f'Unserved and Unfunded Locations in 30 Miles Buffer Ring{extension}')

        state_polygon = os.path.join(save_path, f'{state_name} State Outline{extension}')
        county_polygon = os.path.join(save_path, f'{state_name} Counties Outline{extension}')

        hex_layer = os.path.join(save_path, f'Hex Layer of Unserved and Unfunded Locations{extension}')
        counties_footprint = os.path.join(save_path, f'{state_name} Counties of Footprint{extension}')
        locations_in_counties = os.path.join(save_path, f'Unserved and Unfunded Locations in Counties of FP{extension}')

        federal_grants_path = os.path.join(save_path, f'Federal Grants in {state_name}{extension}')

        ntia_path, rus_path, fcc_path, usac_path, treasury_path = '', '', '', '', '',

        ntia_dataframe = federal_grants[federal_grants['agency_name'] == 'NTIA']
        rus_dataframe = federal_grants[federal_grants['agency_name'] == 'RUS']
        fcc_dataframe = federal_grants[federal_grants['agency_name'] == 'FCC']
        usac_dataframe = federal_grants[federal_grants['agency_name'] == 'USAC']
        treasury_dataframe = federal_grants[federal_grants['agency_name'] == 'Treasury']
        if not ntia_dataframe.empty:
            ntia_path = os.path.join(save_path, f'NTIA Federal Grants in {state_name}{extension}')
            ntia_dataframe.to_file(ntia_path, driver=export_type_dict[extension][0])
        if not rus_dataframe.empty:
            rus_path = os.path.join(save_path, f'RUS Federal Grants in {state_name}{extension}')
            rus_dataframe.to_file(rus_path, driver=export_type_dict[extension][0])
        if not fcc_dataframe.empty:
            fcc_path = os.path.join(save_path, f'FCC Federal Grants in {state_name}{extension}')
            fcc_dataframe.to_file(fcc_path, driver=export_type_dict[extension][0])
        if not usac_dataframe.empty:
            usac_path = os.path.join(save_path, f'USAC Federal Grants in {state_name}{extension}')
            usac_dataframe.to_file(usac_path, driver=export_type_dict[extension][0])
        if not treasury_dataframe.empty:
            treasury_path = os.path.join(save_path, f'Treasury Federal Grants in {state_name}{extension}')
            treasury_dataframe.to_file(treasury_path, driver=export_type_dict[extension][0])

        # locations_in_cb_footprint.to_file(cb_footprint, driver=export_type_dict[extension][0])
        footprint.to_file(cb_footprint, driver=export_type_dict[extension][0])
        ten_mile_dif.to_file(cb_footprint_10, driver=export_type_dict[extension][0])
        thirty_mile_dif.to_file(cb_footprint_30, driver=export_type_dict[extension][0])
        locations_in_cb_footprint.to_file(unserved_unfunded_in_fp, driver=export_type_dict[extension][0])
        locations_in_10mileBuffer.to_file(unserved_unfunded_10, driver=export_type_dict[extension][0])
        locations_in_30mileBuffer.to_file(unserved_unfunded_30, driver=export_type_dict[extension][0])
        state.to_file(state_polygon, driver=export_type_dict[extension][0])
        counties.to_file(county_polygon, driver=export_type_dict[extension][0])

        hex_gdf.to_file(hex_layer, driver=export_type_dict[extension][0])
        counties_fp.to_file(counties_footprint, driver=export_type_dict[extension][0])
        locations_within_counties.to_file(locations_in_counties, driver=export_type_dict[extension][0])

        # federal_grants.to_file(federal_grants_path, driver=export_type_dict[extension][0])

        call_qgis_for_30_10(f'{state_name} BufferedFootprint', state_name, unserved_unfunded_in_fp,
                            unserved_unfunded_10, unserved_unfunded_30, state_polygon, county_polygon, cb_footprint,
                            cb_footprint_10, cb_footprint_30, provider_name, save_path, hex_layer, counties_footprint,
                            locations_in_counties, gradient_path, ntia_path, rus_path, fcc_path, usac_path,
                            treasury_path)
        path_excel = os.path.join(save_path, f'{state_name}_NumberReport.xlsx')

        create_formatted_excel(provider_name, state_name, len(locations_in_cb_footprint),
                               len(locations_in_10mileBuffer),
                               len(locations_in_30mileBuffer), len(locations_within_counties), path_excel,
                               locations_within_counties, locations_in_cb_footprint)
        delete_table_query = f"DROP TABLE IF EXISTS {table_name};"

        # Execute the delete query
        session.execute(text(delete_table_query))
        session.commit()

        session.close()
    finally:
        con = 0


if __name__ == '__main__':
    main()


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


def query_footprint(state_abbr, fips, provider_id, tech_code, max_download, max_upload, br_code):
    query = f"""
        SELECT * FROM
        ww_get_all_cb_polygons_20(ARRAY[{state_abbr}], 
        ARRAY[{fips}], 
        ARRAY[{provider_id}], ARRAY[{tech_code}], 
        ARRAY[{max_download}], ARRAY[{max_upload}], 
        ARRAY[{br_code}]);
    """
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

    return query


def mile_to_meter(miles):
    return miles * 1609.34


def get_hex(provider_id, state, con):
    query = f"""
    SELECT *
    FROM us_fcc_joined_h3_resolution8_test
    WHERE county_fips IN (SELECT
        DISTINCT fips_code
        FROM us_census_block_data cb
        WHERE cb.provider_id = ANY(ARRAY[{provider_id}]) AND cb.state_abbr = ANY(ARRAY[{state}]));
    """
    hex = gpd.GeoDataFrame.from_postgis(query, con, geom_col='geom', crs='EPSG:4326').to_crs('ESRI:102008')

    return hex


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


def get_fip_codes(polygon_data, con, state_abbr):
    query = f"""
        SELECT state_abbr,fips,county_name
        FROM us_counties
        WHERE ST_Intersects(
            geom,
            ST_GeomFromText('{polygon_data.geometry.iloc[0]}', 102008)) AND state_abbr = ANY(ARRAY[{state_abbr}])
    """
    counties = pd.read_sql(query, con)

    return list(counties["fips"].unique())


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


def query_state(state_abbr):
    query = f"""
        SELECT * FROM "USStates" WHERE "StateAbbr" = ANY(ARRAY[{state_abbr}])
    """
    return query


def query_counties(fips):
    query = f"""
        SELECT * FROM us_counties WHERE fips = ANY(ARRAY[{fips}])
    """
    return query


def query_counties_by_provider(provider_id, state):
    query = f"""
    SELECT * 
    FROM us_counties 
    WHERE fips IN 
        (SELECT 
            DISTINCT fips_code 
         FROM us_census_block_data cb 
         WHERE cb.provider_id = ANY(ARRAY[{provider_id}]) AND cb.state_abbr = ANY(ARRAY[{state}]))
    """
    return query


def get_federal_grants(provider_id, state, con):
    query = f"""
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
       gt.categories_served,
       gt.geometry
    FROM us_federal_grants gt
    INNER JOIN agencies ag ON gt.program_id = ag.program_id
    INNER JOIN federal_gt_counties_pivot pivot on gt.id = pivot.grant_id
    INNER JOIN us_counties uc on pivot.county_id = uc.id
    WHERE EXISTS(SELECT 1 
                 FROM us_census_block_data cb 
                 WHERE cb.provider_id = ANY(ARRAY[[{provider_id}]]) 
                   AND cb.state_abbr = ANY(ARRAY[[{state}]])
                   AND cb.fips_code = uc.fips_code)
    """
    gdf = gpd.read_postgis(query, con, geom_col='geometry', crs='ESRI:102008')
    return gdf


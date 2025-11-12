# -*- coding: utf-8 -*-
# %%
from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import sys
from amanda_map_helper import *
from amanda_anchorage_helper import *

fig_fldr = './figures'
vms_anchorages_fldr = '../../pipe_anchorages/data/vms_country_reviewed_lists'

# %% [markdown]
# # Palau

# %%
country_name = 'palau'
df = pd.read_csv(f'{vms_anchorages_fldr}/palau_vms_anchorge.csv')
df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude",
    "s2cell_id": 's2id',
    'sublabel':'iso3'
})
df['sublabel'] = None
df = df[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]
df = clean_overrides(df)
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

# %% [markdown]
# # Papua New Guinea

# %%
country_name = 'papua_new_guinea'
df = pd.read_csv(f'{vms_anchorages_fldr}/png_vms_anchorages.csv')
df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude",
    "s2cell_id": 's2id',
    'sublabel':'iso3'
})
df['sublabel'] = None
df = df[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]
df = clean_overrides(df)
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

# %% [markdown]
# # Peru

# %%
country_name = 'peru'

df = gpd.read_file(f"{vms_anchorages_fldr}/peru_vms_anchorages_s2id_rev_01.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_anchorage_style(row['lat'], row['lon']), axis=1)
df['iso3'] = 'PER'
df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude"
})[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]
df = clean_overrides(df,duplicate_option='keep_last')
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

# %% [markdown]
# # Brazil

# %%
df = pd.read_csv('../../pipe_anchorages/data/port_lists/brazil_original_overrides.csv')
df = clean_overrides(df,duplicate_option='keep_last')
df.to_csv('../../pipe_anchorages/data/port_lists/brazil_overrides.csv',index=False)


# %% [markdown]
# # Chile

# %%
# ran once to swap the labels and sublabels to align with the other overrides where label is the most specific and sublabel is the larger region
# chl_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/chile_overrides.csv')

# chl_anchorages['label'], chl_anchorages['sublabel'] = (
#     chl_anchorages['sublabel'].copy(),
#     chl_anchorages['label'].copy()
# )
# chl_anchorages.to_csv('../../pipe_anchorages/data/port_lists/chile_overrides.csv', index = False)

# %%
country_name = 'chile'
df_singleS2 = pd.read_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv')
#df_to_bq(df,'scratch_amanda_ttl_120',f"reviewed_ports_{country_name}",v='',project_id = 'world-fishing-827',if_exists = 'fail') # fail -> replace if you want to overwrite
df = run_buffer_1km_s2cells_query(country_name)
df = clean_overrides(df)
df
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_overrides.csv', index = False)

color_map = {
    "provided_points": "purple",
    "buffer": "orange",
}
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True, color_map = color_map)
m.save(f"{fig_fldr}/buffered_S2cell_overrides_map_{country_name}_20251024.html")
m


# %% [markdown]
# # Panama

# %%
country_name = 'panama'

df = gpd.read_file("../../pipe_anchorages/data/country_reviewed_lists/panama_vms_anchorages_s2id_rev01.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_level14_hex8(row['lat'], row['lon']), axis=1)
df['iso3'] = 'PAN'
df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude"
})[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]
#df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv',index=False)


# %%
df_singleS2 = pd.read_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv')
#df_to_bq(df,'scratch_amanda_ttl_120',f"reviewed_ports_{country_name}",v='',project_id = 'world-fishing-827',if_exists = 'fail') # fail -> replace if you want to overwrite
df = run_buffer_1km_s2cells_query(country_name)
df = clean_overrides(df)
df
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_overrides.csv', index = False)

color_map = {
    "provided_points": "purple",
    "buffer": "orange",
}
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True, color_map = color_map)
m.save(f"{fig_fldr}/buffered_S2cell_overrides_map_{country_name}_20251024.html")
m

# %% [markdown]
# # Ecuador

# %%
country_name = 'ecuador'
df = gpd.read_file("../../pipe_anchorages/data/country_reviewed_lists/ecuador_vms_anchorages_s2id_rev01.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_level14_hex8(row['LATITUD'], row['LONGITUD']), axis=1)
df['iso3'] = 'ECU'
df = df.rename(columns={
    "LATITUD": "latitude",
    "LONGITUD": "longitude"
})[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]
# df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv', index = False)
df

# %%
df_singleS2 = pd.read_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv')
#df_to_bq(df,'scratch_amanda_ttl_120',f"reviewed_ports_{country_name}",v='',project_id = 'world-fishing-827',if_exists = 'fail') # fail -> replace if you want to overwrite
df = run_buffer_1km_s2cells_query(country_name)
df = clean_overrides(df)
df
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_overrides.csv', index = False)

color_map = {
    "provided_points": "purple",
    "buffer": "orange",
}
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True, color_map = color_map)
m.save(f"{fig_fldr}/buffered_S2cell_overrides_map_{country_name}_20251024.html")
m

# %% [markdown]
# # Costa Rica

# %%
country_name = 'costa_rica'
df = gpd.read_file("../../pipe_anchorages/data/country_reviewed_lists/costa_rica_vms_anchorages_s2id_rev02.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_level14_hex8(row['lat'], row['lon']), axis=1)


## get iso3 (the costa rica anchorages are in multiple countries, not just costa rica)

gdf_points = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df['lon'], df['lat']),
    crs="EPSG:4326"   # lat/lon WGS84
)
eez = gpd.read_file("./EEZ_land_union_v4_202410/EEZ_land_union_v4_202410.shp")[["ISO_TER1", "geometry"]]
eez = eez.rename(columns={"ISO_TER1": "iso3"})
eez["iso3"] = eez["iso3"].replace({None: "high_seas"})
np.unique(eez['iso3'])


# Ensure both are in the same CRS
gdf_points = gdf_points.to_crs(eez.crs)

# Spatial join — assigns EEZ iso3 code to each point
gdf_joined = gpd.sjoin(gdf_points, eez, how="left", op="within")

# Clean up — move iso3 column into your DataFrame
df["iso3"] = gdf_joined["iso3"]
df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude"
})[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]
#df.to_csv('../../pipe_anchorages/data/port_lists/costa_rica_singleS2cell_overrides.csv',index=False)
df

# %%
df_singleS2 = pd.read_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv')
#df_to_bq(df,'scratch_amanda_ttl_120',f"reviewed_ports_{country_name}",v='',project_id = 'world-fishing-827',if_exists = 'fail') # fail -> replace if you want to overwrite
df = run_buffer_1km_s2cells_query(country_name)
df = clean_overrides(df)
df
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_overrides.csv', index = False)

color_map = {
    "provided_points": "purple",
    "buffer": "orange",
}
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True, color_map = color_map)
m.save(f"{fig_fldr}/buffered_S2cell_overrides_map_{country_name}_20251024.html")
m



# %%

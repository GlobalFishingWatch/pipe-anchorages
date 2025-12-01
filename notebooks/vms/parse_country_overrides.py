# %%
from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import sys
from amanda_map_helper import *
from amanda_anchorage_helper import *

import json


vms_anchorages_fldr = '../../pipe_anchorages/data/vms_country_reviewed_lists'

FIG_FLDR = './figures'
PORT_LIST_FLDR = '../../pipe_anchorages/data/port_lists'

# %%
def make_overrides(df, country_name, duplicate_option = 'nothing', overwrite=False, display_map=False,):
    print(f'Getting overrides for {country_name}...')
    overrides_file = f'{PORT_LIST_FLDR}/{country_name}_vms_overrides.csv'
    map_file = f"{FIG_FLDR}/vms_overrides_map_{country_name}.html"
    
    df = clean_overrides(df,duplicate_option=duplicate_option)

    save_files = overwrite or (not os.path.exists(overrides_file))

    if save_files:
            df.to_csv(overrides_file, index=False)
            print(f"Saved overrides to {overrides_file}")
    else:
        print(f"File {overrides_file} already exists. Set overwrite=True to overwrite it")


    if save_files or display_map:
        df['source'] = f'{country_name}_vms_overrides'
        m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)

        if save_files:
            m.save(map_file)
            print(f"Saved map to {map_file}")

        if display_map:
            display(m)

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

make_overrides(df,country_name,overwrite=False,display_map=True)


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

make_overrides(df,country_name,overwrite=False,display_map=True)


# %% [markdown]
# # Peru

# %%
country_name = 'peru'

df = gpd.read_file(f"{vms_anchorages_fldr}/peru_vms_anchorages_s2id_rev_01.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_anchorage_style(row['lat'], row['lon']), axis=1)
df['iso3'] = 'PER'
df['sublabel'] = None # clear sublabel
df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude"
})[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]

make_overrides(df,country_name,overwrite=True,display_map=True,duplicate_option='keep_last')

# %% [markdown]
# # Brazil

# %% [markdown]
# ### duplicate map

# %%
# country_name = 'brazil'
# df = pd.read_csv(f'{vms_anchorages_fldr}/brazil_original_overrides.csv')
# df = clean_overrides(df,duplicate_option='combine_with_ampersand')
# df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)

# df['source'] = np.where(
#     df['label'].str.contains('&', na=False),
#     f'{country_name}_vms_overrides_DUPLICATE',
#     f'{country_name}_vms_overrides'
# )

# #df = df[df['source'] == f'{country_name}_vms_overrides_DUPLICATE'].reset_index(drop=True)

# color_map = {
#     f'{country_name}_vms_overrides':'blue',
#     f'{country_name}_vms_overrides_DUPLICATE': "orange",
# }

# m = map_s2_anchorages(df, show_labels=False, fit_bounds=True, color_map = color_map)
# m.save(f"{fig_fldr}/vms_overrides_duplicates_map_{country_name}_20251110.html")
# m

# %%
country_name = 'brazil'
df = pd.read_csv(f'{vms_anchorages_fldr}/brazil_original_overrides.csv')
sao_jose_s2ids = ['9511839d','95118377','95118371']

# Remove specific duplicates - keep Cabo over Jaboatao
conflict_ids = (
    df.groupby('s2id')['label']
      .agg(lambda x: {'Cabo de Santo Agostinho', 'Jaboatão dos Guararapes'}.issubset(set(x)))
)
conflict_ids = conflict_ids[conflict_ids].index
df = df[
    ~df['s2id'].isin(conflict_ids) | (df['label'] == 'Cabo de Santo Agostinho')
].reset_index(drop=True)

# Remove specific duplicates - keep São José do Norte over Rio Grande do Sul
conflict_ids = (
    df.groupby('s2id')['label']
      .agg(lambda x: {'São José do Norte', 'Rio Grande'}.issubset(set(x)))
)
conflict_ids = conflict_ids[conflict_ids].index
df = df[
    ~df['s2id'].isin(conflict_ids) | (df['label'] == 'São José do Norte')
].reset_index(drop=True)

df['sublabel'] = None # clear sublabels
make_overrides(df,country_name,overwrite=True,display_map=True,duplicate_option='keep_last')

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
df = pd.read_csv(f'{vms_anchorages_fldr}/chile_original_overrides.csv')
df['sublabel'] = None # clear sublabels
make_overrides(df,country_name,overwrite=True,display_map=True,duplicate_option='combine_with_ampersand')

# %% [markdown]
# # Panama

# %%
country_name = 'panama'

df = gpd.read_file(f"{vms_anchorages_fldr}/panama_vms_anchorages_s2id_rev01.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_anchorage_style(row['lat'], row['lon']), axis=1)
df['iso3'] = 'PAN'
df = df.rename(columns={
    "lat": "latitude",
    "lon": "longitude"
})[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]

df['sublabel'] = None # clear sublabels

make_overrides(df,country_name,overwrite=True,display_map=True,duplicate_option='combine_with_ampersand')

# %% [markdown]
# # Ecuador

# %%
country_name = 'ecuador'
df = gpd.read_file(f"{vms_anchorages_fldr}/ecuador_vms_anchorages_s2id_rev01.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_anchorage_style(row['LATITUD'], row['LONGITUD']), axis=1)
df['iso3'] = 'ECU'
df = df.rename(columns={
    "LATITUD": "latitude",
    "LONGITUD": "longitude"
})[["s2id", "latitude", "longitude", "label", "sublabel", "iso3"]]

df['sublabel'] = None # clear sublabels

make_overrides(df,country_name,overwrite=True,display_map=True,duplicate_option='combine_with_ampersand')

# %% [markdown]
# # Costa Rica

# %%
country_name = 'costa_rica'
df = gpd.read_file(f"{vms_anchorages_fldr}/costa_rica_vms_anchorages_s2id_rev02.geojson")
df = pd.DataFrame(df)
df['s2id'] = df.apply(lambda row: s2_anchorage_style(row['lat'], row['lon']), axis=1)


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

df['sublabel'] = None # clear sublabels

make_overrides(df,country_name,overwrite=True,display_map=True,duplicate_option='combine_with_ampersand')



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

import json


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
#df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

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

df = clean_overrides(df,duplicate_option='keep_last')
#df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
#m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

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

# %% [markdown]
# ### duplicate map

# %%
from typing import Dict, Any, List
import pandas as pd
from s2sphere import Cell, CellId, LatLng

def df_to_s2_feature_collection(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Convert a DataFrame with S2 cell tokens to a GeoJSON FeatureCollection of polygons.

    Expected columns:
      - s2id (str): S2 cell token
      - label (optional, str)
      - sublabel (optional, str)
      - source (optional, str)
      - s2lat, s2lon (optional, floats) — only tracked for potential fit_bounds use

    Rows with invalid/missing s2id tokens are skipped.
    """
    features: List[Dict[str, Any]] = []
    lon_all: List[float] = []  # retained in case you want fit_bounds/bbox later
    lat_all: List[float] = []

    for row in df.itertuples(index=False):
        s2id = getattr(row, "s2id", None)
        if s2id is None or (isinstance(s2id, float) and pd.isna(s2id)):
            continue

        label = getattr(row, "label", "NULL")
        sublabel = getattr(row, "sublabel", "NULL")
        src = getattr(row, "source", "no_source")
        label_src = getattr(row,"label_and_source","no label_and_source")

        # Try constructing the S2 cell; skip invalids
        try:
            cell = Cell(CellId.from_token(str(s2id)))
        except Exception:
            continue

        # Build the cell polygon ring (lon, lat) and close it
        ring: List[List[float]] = []
        for i in range(4):
            v = cell.get_vertex(i)
            ll = LatLng.from_point(v)
            ring.append([ll.lng().degrees, ll.lat().degrees])
            lon_all.append(ll.lng().degrees)
            lat_all.append(ll.lat().degrees)
        ring.append(ring[0])

        # Track centroids too for potential fit_bounds
        if hasattr(row, "s2lat") and hasattr(row, "s2lon"):
            lat_all.append(getattr(row, "s2lat"))
            lon_all.append(getattr(row, "s2lon"))

        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {
                "label": label if pd.notna(label) else "NULL",
                "sublabel": sublabel if pd.notna(sublabel) else "NULL",
                "source": src if pd.notna(src) else "no_source",
                "label_source": label_src if pd.notna(label_src) else "no label_and_source",
                "s2id": s2id
            }
        })

    # Build and return FeatureCollection
    return {"type": "FeatureCollection", "features": features, "tolerance": 0}

# %%
country_name = 'chile'
df = pd.read_csv(f'{vms_anchorages_fldr}/chile_original_overrides.csv')
df = clean_overrides(df,duplicate_option='combine_with_ampersand')

df['source'] = np.where(
    df['label'].str.contains('&', na=False),
    f'{country_name}_vms_overrides_DUPLICATE',
    f'{country_name}_vms_overrides'
)

color_map = {
    f'{country_name}_vms_overrides':'blue',
    f'{country_name}_vms_overrides_DUPLICATE': "orange",
}

m = map_s2_anchorages(df, show_labels=False, fit_bounds=True, color_map = color_map)
m.save(f"{fig_fldr}/vms_overrides_duplicates_map_{country_name}_20251110.html")
m

# %%
df_reg = df[df['source']==f'{country_name}_vms_overrides'].reset_index(drop=True)
df_dup = df[df['source']==f'{country_name}_vms_overrides_DUPLICATE'].reset_index(drop=True)

polys = df_to_s2_feature_collection(df_reg)
# Save the FeatureCollection to a GeoJSON file
output_path = f"{country_name}_nondup_anchorages.geojson"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(polys, f, ensure_ascii=False, indent=2)
print(f"Saved {output_path} ({len(polys['features'])} features)")

polys = df_to_s2_feature_collection(df_dup)
# Save the FeatureCollection to a GeoJSON file
output_path = f"{country_name}_dup_anchorages.geojson"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(polys, f, ensure_ascii=False, indent=2)
print(f"Saved {output_path} ({len(polys['features'])} features)")

# %% [markdown]
# ### regular map

# %%
country_name = 'chile'
df = pd.read_csv(f'{vms_anchorages_fldr}/chile_original_overrides.csv')
df = clean_overrides(df,duplicate_option='combine_with_ampersand')
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

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
df = clean_overrides(df,duplicate_option='keep_last')
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

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
df = clean_overrides(df,duplicate_option='keep_last')
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

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

df = clean_overrides(df,duplicate_option='keep_last')
df.to_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_vms_overrides.csv',index=False)
df['source'] = f'{country_name}_vms_overrides'
m = map_s2_anchorages(df, show_labels=False, fit_bounds=True)
m.save(f"{fig_fldr}/vms_overrides_map_{country_name}_20251110.html")
m

# %%
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature


def plot_point_on_map(latlon, zoom=False):
    
    """
    Plot a (lat, lon) point on a static world map.

    Args:
        lat (float): Latitude in decimal degrees.
        lon (float): Longitude in decimal degrees.
        zoom (bool): If True, zooms in around the point.
    """
    lat = latlon[0]
    lon = latlon[1]
    # Set up the map projection
    proj = ccrs.PlateCarree()
    fig = plt.figure(figsize=(10, 5))
    ax = plt.axes(projection=proj)

    # Add map features
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.COASTLINE)

    # Plot the point
    ax.plot(lon, lat, 'ro', markersize=8, transform=proj)
    ax.text(lon + 3, lat, f'({lat:.2f}, {lon:.2f})', transform=proj)

    # Optionally zoom in
    if zoom:
        ax.set_extent([lon - 5, lon + 5, lat - 5, lat + 5], crs=proj)
    else:
        ax.set_global()

    ax.set_title("Point on World Map")
    plt.show()

# %%
plot_point_on_map(s2id_to_latlon('50000001'))

# %%
plot_point_on_map(s2id_to_latlon('5aaaaaab'))

# %%
plot_point_on_map(s2id_to_latlon('140edc01'))


# %%
s2id_to_latlon('140edc01')



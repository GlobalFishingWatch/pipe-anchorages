# -*- coding: utf-8 -*-
# %%
from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import json


from amanda_anchorage_helper import *
fig_fldr = './figures'


# %% [markdown]
# # Merge anchorages_overrides with the vms country overrides

# %%
from s2sphere import CellId, LatLng

def s2_level14_hex8(lat: float, lon: float) -> str:
    # Build the level-14 S2 cell containing this point
    cid = CellId.from_lat_lng(LatLng.from_degrees(lat, lon)).parent(14)
    # Format the 64-bit id as 16 hex chars and take the first 8 (most significant)
    return f"{cid.id():016x}"[:8]




# %% [markdown]
# ## Read AIS anchorage overrides

# %%
ais_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/anchorage_overrides.csv')
ais_anchorages['label_source'] = 'anchorage_overrides'
ais_anchorages = clean_overrides(ais_anchorages)
ais_anchorages

# %% [markdown]
# ## Brazil

# %%
bra_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/brazil_overrides.csv')
bra_anchorages['label_source'] = 'brazil_vms_overrides'
bra_anchorages = clean_overrides(bra_anchorages, True)
bra_anchorages

# %% [markdown]
# ## Chile

# %%
chl_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/chile_overrides.csv')
chl_anchorages['label_source'] = 'chile_vms_overrides'
chl_anchorages = clean_overrides(chl_anchorages, True)
chl_anchorages


# %% [markdown]
# ## Panama

# %%
pan_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/panama_overrides.csv')
pan_anchorages['label_source'] = 'panama_vms_overrides'
pan_anchorages = clean_overrides(pan_anchorages, True)
pan_anchorages

# %% [markdown]
# ## Ecuador

# %%
ecu_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/ecuador_overrides.csv')
ecu_anchorages['label_source'] = 'ecuador_vms_overrides'
ecu_anchorages = clean_overrides(ecu_anchorages, True)
ecu_anchorages

# %% [markdown]
# ## Costa Rica 

# %%
cri_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/costa_rica_overrides.csv')
cri_anchorages['label_source'] = 'costa_rica_vms_overrides'
cri_anchorages = clean_overrides(cri_anchorages, True)
cri_anchorages

# %% [markdown]
# ## Merge

# %%
combined_anchorages = pd.concat([bra_anchorages, chl_anchorages, pan_anchorages, ecu_anchorages, cri_anchorages])
old_len = len(combined_anchorages)
duplicates = combined_anchorages[combined_anchorages.duplicated(subset='s2id', keep=False)]
combined_anchorages = combined_anchorages.drop_duplicates(subset='s2id', keep='first').reset_index(drop=True) # taking first because this takes ecuador over costa rica which has anchorages in ecuador
if old_len - len(combined_anchorages) > 0:
    print(f"WARNING: Dropped {old_len - len(combined_anchorages)} duplicates from country-reviewed lists. This means one country-reviewed list overwrote at least 1 row from another. You should look into this")

combined_anchorages = pd.concat([ais_anchorages, combined_anchorages])
old_len = len(combined_anchorages)
combined_anchorages = combined_anchorages.drop_duplicates(subset='s2id', keep='last').reset_index(drop=True)
print(f"Dropped {old_len - len(combined_anchorages)} s2ids from AIS overrides list that were duplicated in the country-reviewed lists")

print('Country duplicates:')
duplicates

# %%
# combined_anchorages.to_csv('../../pipe_anchorages/data/port_lists/combined_anchorage_overrides.csv',index=False)


# %%
combined_anchorages

# %% [markdown]
# # map anchorages

# %% [markdown]
# ## static maps

# %%
q = f'''
SELECT
  *
FROM
  `world-fishing-827.scratch_amanda_ttl_120.combined_named_anchorages_precursor_v20251022`
'''
df = get_bq_df(q)
df['s2lat'], df['s2lon'] = zip(*df['s2id'].map(s2id_to_latlon))
# Left join on 's2id' to bring in the 'source' column
df = df.merge(
    combined_anchorages[["s2id", "source"]],
    on="s2id",
    how="left"
)

# Fill missing (non-matching) values with "ais_detected"
df["source"] = df["source"].fillna("ais_detected")

df

# %%
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import contextily as ctx
from pyproj import Transformer


focal_country_names = ['Brazil','Chile','Ecuador','Costa Rica','Panama']
#focal_country_names = ['Ecuador','Costa Rica','Panama']

target_anchorages = df.copy()

for focal_country_name in focal_country_names:
    # 1) Build a GeoDataFrame from df['s2lat'], df['s2lon']
    #    (drops rows with missing/invalid coords)
    gdf = gpd.GeoDataFrame(
        target_anchorages,
        geometry=gpd.points_from_xy(target_anchorages['s2lon'], target_anchorages['s2lat']),
        crs="EPSG:4326"
    )

    # 2) Get focal_country polygon and bounds (Natural Earth included with GeoPandas)
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    focal_country = world[world['name'] == focal_country_name].to_crs(3857)  # Web Mercator for basemap

    # 3) Project points to Web Mercator
    gdf = gdf.to_crs(3857)

    # Optional: pad the map extent a bit around focal_country
    xmin, ymin, xmax, ymax = focal_country.total_bounds
    pad_x = (xmax - xmin) * 0.05
    pad_y = (ymax - ymin) * 0.05
    extent = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

    # 4) Plot
    fig, ax = plt.subplots(figsize=(10, 10))

    # (Optional) outline focal_country for context
    # focal_country.boundary.plot(ax=ax, linewidth=0.8, color="black", alpha=0.7)

    # Updated color map (balanced, high-contrast colors)
    color_map = {
        'brazil_vms_reviewed':      "#1f78b4",  # blue
        'chile_vms_reviewed':       "#33a02c",  # green
        'panama_vms_reviewed':         "#ff7f00",  # orange
        'costa_rica_vms_reviewed':         "#6a3d9a",  # purple
        'ecuador_vms_reviewed':         "#e31a1c",  # red
        'ais_anchorage_overrides':  "#A57777",  # gray
        'ais_detected':  "#414141",  # gray
    }

    label_map = {
        'brazil_vms_reviewed':     'Brazil reviewed anchorages',
        'chile_vms_reviewed':      'Chile reviewed anchorages',
        'panama_vms_reviewed':        'Panama reviewed anchorages',
        'costa_rica_vms_reviewed':        'Costa Rica reviewed anchorages',
        'ecuador_vms_reviewed':        'Ecuador reviewed anchorages',
        'ais_anchorage_overrides': 'AIS anchorage overrides',
        'ais_detected': 'Detected from AIS data'
    }

    # Desired plotting order (from larger EEZs / regional grouping)
    plot_order = [
        'brazil_vms_reviewed',
        'chile_vms_reviewed',
        'ecuador_vms_reviewed',
        'panama_vms_reviewed',
        'costa_rica_vms_reviewed',
        'ais_anchorage_overrides',
        'ais_detected'
    ]


    # Plot groups in controlled order
    for source in plot_order:
        group = gdf[gdf['source'] == source]
        if not group.empty:
            color = color_map.get(source, 'gray')
            label = label_map.get(source, source)
            group.plot(ax=ax, markersize=15, color=color, alpha=0.7, edgecolor = "white", linewidth=0.2, label=label)

    # Plot any remaining sources not listed
    remaining_sources = [s for s in gdf['source'].unique() if s not in plot_order]
    for source in remaining_sources:
        group = gdf[gdf['source'] == source]
        color = color_map.get(source, 'gray')
        label = label_map.get(source, source)
        group.plot(ax=ax, markersize=15, color=color, alpha=0.7, edgecolor = "white", linewidth=0.2, label=label)

    # # Plot points (choose a high-contrast, colorblind-friendly color)
    # gdf_3857.plot(
    #     ax=ax,
    #     markersize=15,
    #     color="#0072B2",      # blue (CUD palette)
    #     alpha=0.85,
    #     edgecolor="white",
    #     linewidth=0.2
    # )



    # Zoom to Brazil
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])

    # Add a clean basemap

    # Create a transformer from 3857 → 4326
    transformer = Transformer.from_crs(3857, 4326, always_xy=True)

    # Transform all corners
    minlon, minlat = transformer.transform(extent[0], extent[2])
    maxlon, maxlat = transformer.transform(extent[1], extent[3])


    z = ctx.tile._calculate_zoom(minlon, minlat, maxlon, maxlat)

    if focal_country_name not in ['Brazil','Chile']:
        z = z + 1
    ctx.add_basemap(
        ax,
        source=ctx.providers.CartoDB.Positron,
        zoom=z,                 # ← key to sharpness
    )

    # Tidy labels
    ax.set_title(f"All anchorages (Zoomed to {focal_country_name})")
    ax.set_axis_off()

    # Place legend outside the map (right side)
    ax.legend(
        loc="center left",            # position legend to the left of the anchor box
        bbox_to_anchor=(1.02, 0.5),   # (x, y): just outside the right edge, vertically centered
        frameon=True,
        fontsize=9,
        title="Source",
        title_fontsize=10
    )

    # Save high-res image
    plt.savefig(f"./figures/all_anchorages_by_source_{focal_country_name}.png", dpi=600, bbox_inches="tight", facecolor="white")

    plt.show()

# %% [markdown]
# ## folium html maps

# %%
combined_anchorages = combined_anchorages.rename(columns={'label_source': 'source'})

# %%
q = f'''
SELECT
  *
FROM
  `world-fishing-827.scratch_amanda_ttl_120.combined_named_anchorages_precursor_v20251024`
'''
df = get_bq_df(q)
df['s2lat'], df['s2lon'] = zip(*df['s2id'].map(s2id_to_latlon))
df

# %%

# Left join on 's2id' to bring in the 'source' column
df = df.merge(
    combined_anchorages[["s2id", "source"]],
    on="s2id",
    how="left"
)

# Fill missing (non-matching) values with "ais_detected"
df["source"] = df["source"].fillna("ais_detected")

df

# %%
country_names = ['costa_rica','ecuador','panama','chile']

for country_name in country_names:
    df.loc[df["source"] == f"{country_name}_vms_overrides", "source"] = f"{country_name}_vms_overrides_buffer"
    df1 = pd.read_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv')
    df.loc[df["s2id"].isin(df1["s2id"]), "source"] = f"{country_name}_vms_overrides_provided_point"

np.unique(df['source'])

# %%
country_iso = 'BRA'
country_name = 'brazil'


dfc = df[df['iso3'] == country_iso].reset_index(drop=True)

color_map = {
    f"{country_name}_vms_reviewed":"orange",
    "ais_anchorage_overrides": "blue",
    "ais_detected": "darkblue",
}

label_map = {
    f"{country_name}_vms_reviewed":f"{country_name.replace('_', ' ').title()} reviewed",
    "ais_anchorage_overrides": "AIS anchorage overrides",
    "ais_detected": "Detected from AIS",
}

m = map_s2_anchorages(dfc,color_map = color_map, label_map = label_map, legend_title='Source (first on this list that applies)')
m.save(f"{fig_fldr}/{country_name}_combined_named_anchorages.html")
m

# %%
# ecuador also has costa rica reviewed anchorages
country_name = 'ecuador'
country_iso = 'ECU'

dfc = df[df['iso3'] == country_iso].reset_index(drop=True)

color_map = {
    f"{country_name}_vms_reviewed_provided_point":"darkred",
    f"{country_name}_vms_reviewed_buffer":"orange",
    "costa_rica_vms_reviewed_provided_point": "purple",
    "costa_rica_vms_reviewed_buffer": "lavender",
    "ais_anchorage_overrides": "blue",
    "ais_detected": "darkblue",
}

label_map = {
    f"{country_name}_vms_reviewed_provided_point":f"{country_name.replace('_', ' ').title()} reviewed - provided point",
    f"{country_name}_vms_reviewed_buffer":f"{country_name.replace('_', ' ').title()} reviewed - 1km buffer",
    "costa_rica_vms_reviewed_provided_point": "Costa Rica reviewed - 1km buffer",
    "costa_rica_vms_reviewed_buffer": "Costa Rica reviewed - 1km buffer",
    "ais_anchorage_overrides": "AIS anchorage overrides",
    "ais_detected": "Detected from AIS",
}

m = map_s2_anchorages(dfc,color_map = color_map, label_map = label_map, legend_title='Source (first on this list that applies)')
m.save(f"{fig_fldr}/{country_name}_combined_named_anchorages.html")
m

# %%
country_iso = 'PAN'
country_name = 'panama'


dfc = df[df['iso3'] == country_iso].reset_index(drop=True)

color_map = {
    f"{country_name}_vms_reviewed_provided_point":"darkred",
    f"{country_name}_vms_reviewed_buffer":"orange",
    "ais_anchorage_overrides": "blue",
    "ais_detected": "darkblue",
}

label_map = {
    f"{country_name}_vms_reviewed_provided_point":f"{country_name.replace('_', ' ').title()} reviewed - provided point",
    f"{country_name}_vms_reviewed_buffer":f"{country_name.replace('_', ' ').title()} reviewed - 1km buffer",
    "ais_anchorage_overrides": "AIS anchorage overrides",
    "ais_detected": "Detected from AIS",
}

m = map_s2_anchorages(dfc,color_map = color_map, label_map = label_map, legend_title='Source (first on this list that applies)')
m.save(f"{fig_fldr}/{country_name}_combined_named_anchorages.html")
m

# %%
country_iso = 'CRI'
country_name = 'costa_rica'


dfc = df[df['iso3'] == country_iso].reset_index(drop=True)

color_map = {
    f"{country_name}_vms_reviewed_provided_point":"darkred",
    f"{country_name}_vms_reviewed_buffer":"orange",
    "ais_anchorage_overrides": "blue",
    "ais_detected": "darkblue",
}

label_map = {
    f"{country_name}_vms_reviewed_provided_point":f"{country_name.replace('_', ' ').title()} reviewed - provided point",
    f"{country_name}_vms_reviewed_buffer":f"{country_name.replace('_', ' ').title()} reviewed - 1km buffer",
    "ais_anchorage_overrides": "AIS anchorage overrides",
    "ais_detected": "Detected from AIS",
}

m = map_s2_anchorages(dfc,color_map = color_map, label_map = label_map, legend_title='Source (first on this list that applies)')
m.save(f"{fig_fldr}/{country_name}_combined_named_anchorages.html")
m

# %%
country_iso = 'CHL'
country_name = 'chile'


dfc = df[df['iso3'] == country_iso].reset_index(drop=True)

color_map = {
    f"{country_name}_vms_reviewed_provided_point":"darkred",
    f"{country_name}_vms_reviewed_buffer":"orange",
    "ais_anchorage_overrides": "blue",
    "ais_detected": "darkblue",
}

label_map = {
    f"{country_name}_vms_reviewed_provided_point":f"{country_name.replace('_', ' ').title()} reviewed - provided point",
    f"{country_name}_vms_reviewed_buffer":f"{country_name.replace('_', ' ').title()} reviewed - 1km buffer",
    "ais_anchorage_overrides": "AIS anchorage overrides",
    "ais_detected": "Detected from AIS",
}

m = map_s2_anchorages(dfc,color_map = color_map, label_map = label_map, legend_title='Source (first on this list that applies)')
m.save(f"{fig_fldr}/{country_name}_combined_named_anchorages.html")
m

# %% [markdown]
# # Save polygons

# %%
df["label_and_source"] = df["label"].astype(str) + ": " + df["source"].astype(str)
df

# %%
np.unique(df['source'])

# %%
dfc

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

# %%
import json

country_names = ['costa_rica', 'ecuador', 'panama', 'chile', 'brazil']
country_isos = ['CRI', 'ECU', 'PAN', 'CHL', 'BRA']

for country_name, country_iso in zip(country_names, country_isos):
    dfc = df[df["iso3"] == country_iso]
    polys = df_to_s2_feature_collection(dfc)

    # Save the FeatureCollection to a GeoJSON file
    output_path = f"{country_name}_anchorages.geojson"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(polys, f, ensure_ascii=False, indent=2)

    print(f"Saved {output_path} ({len(polys['features'])} features)")

# %%

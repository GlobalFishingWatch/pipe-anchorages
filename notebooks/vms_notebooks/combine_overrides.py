# %%
from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx

from amanda_anchorage_helper import *
fig_fldr = './figures'


# %% [markdown]
# # label comparison
# Are the labels in vms_anchorage_overrides.csv the same as the ones in the table Claudino shared?

# %%
df = pd.read_csv('../../pipe_anchorages/data/port_lists/vms_anchorage_overrides.csv')
df

# %%
labels_vms_overrides = df[['label', 'sublabel']].drop_duplicates().reset_index(drop=True)
labels_vms_overrides

# %%
q = f'''
SELECT 
DISTINCT
  NM_MUN as label, -- city name
  NM_UF as sublabel -- state name
FROM 
  `world-fishing-827.scratch_claudino.bra_ports_rev_2025_02`
'''
df = get_bq_df(q)
labels_claudino = df


# %%
diff = (
    labels_vms_overrides
    .merge(labels_claudino, on=['label', 'sublabel'], how='outer', indicator=True)
    .query('_merge != "both"')
    .drop(columns=['_merge'])
)
diff

# %% [markdown]
# Conclusion: labels and sublabels are the same in both, so no need to update vms_anchorages_overrides.csv

# %% [markdown]
# # Merge anchorages_overrides with vms_anchorage_overrides

# %%
from s2sphere import CellId, LatLng

def s2_level14_hex8(lat: float, lon: float) -> str:
    # Build the level-14 S2 cell containing this point
    cid = CellId.from_lat_lng(LatLng.from_degrees(lat, lon)).parent(14)
    # Format the 64-bit id as 16 hex chars and take the first 8 (most significant)
    return f"{cid.id():016x}"[:8]




# %% [markdown]
# ## Read AIS anchorages

# %%
ais_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/anchorage_overrides.csv')
ais_anchorages['source'] = 'ais_anchorage_overrides'
ais_anchorages = clean_overrides(ais_anchorages)
ais_anchorages

# %% [markdown]
# ## Brazil

# %%
bra_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/brazil_overrides.csv')
bra_anchorages['source'] = 'brazil_vms_reviewed'
bra_anchorages = clean_overrides(bra_anchorages, True)
bra_anchorages

# %% [markdown]
# ## Chile

# %%
chl_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/chile_overrides.csv')
chl_anchorages['source'] = 'chile_vms_reviewed'
chl_anchorages = clean_overrides(chl_anchorages, True)
chl_anchorages


# %% [markdown]
# ## Panama

# %%
pan_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/panama_overrides.csv')
pan_anchorages['source'] = 'panama_vms_reviewed'
pan_anchorages = clean_overrides(pan_anchorages, True)
pan_anchorages

# %% [markdown]
# ## Ecuador

# %%
ecu_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/ecuador_overrides.csv')
ecu_anchorages['source'] = 'ecuador_vms_reviewed'
ecu_anchorages = clean_overrides(ecu_anchorages, True)
ecu_anchorages

# %% [markdown]
# ## Costa Rica 

# %%
cri_anchorages = pd.read_csv('../../pipe_anchorages/data/port_lists/costa_rica_overrides.csv')
cri_anchorages['source'] = 'costa_rica_vms_reviewed'
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
combined_anchorages_no_source = combined_anchorages.drop(columns='source')
# combined_anchorages_no_source.to_csv('../../pipe_anchorages/data/port_lists/combined_anchorage_overrides.csv',index=False)


# %% [markdown]
# # map anchorages

# %%
target_anchorages = combined_anchorages.copy()
target_anchorages

# %%
# Convert s2id to lat/lon (centroid of each cell)

def s2id_to_latlon(s2id):
    # Handle string (token)
    cell = CellId.from_token(s2id)

    latlng = cell.to_lat_lng()
    return latlng.lat().degrees, latlng.lng().degrees

target_anchorages['s2lat'], target_anchorages['s2lon'] = zip(*target_anchorages['s2id'].map(s2id_to_latlon))

# %%
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import contextily as ctx

focal_country_names = ['Brazil','Chile','Ecuador','Costa Rica','Panama']

for focal_country_name in focal_country_names:
    # 1) Build a GeoDataFrame from df['s2lat'], df['s2lon']
    #    (drops rows with missing/invalid coords)
    gdf = gpd.GeoDataFrame(
        target_anchorages,
        geometry=gpd.points_from_xy(target_anchorages['s2lon'], target_anchorages['s2lat']),
        crs="EPSG:4326"
    )

    # 2) Get Brazil polygon and bounds (Natural Earth included with GeoPandas)
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    focal_country = world[world['name'] == focal_country_name].to_crs(3857)  # Web Mercator for basemap

    # 3) Project points to Web Mercator
    gdf = gdf.to_crs(3857)

    # Optional: pad the map extent a bit around Brazil
    xmin, ymin, xmax, ymax = focal_country.total_bounds
    pad_x = (xmax - xmin) * 0.05
    pad_y = (ymax - ymin) * 0.05
    extent = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

    # 4) Plot
    fig, ax = plt.subplots(figsize=(10, 10))

    # (Optional) outline Brazil for context
    focal_country.boundary.plot(ax=ax, linewidth=0.8, color="black", alpha=0.7)

    # Updated color map (balanced, high-contrast colors)
    color_map = {
        'brazil_vms_reviewed':      "#1f78b4",  # blue
        'chile_vms_reviewed':       "#33a02c",  # green
        'panama_vms_reviewed':         "#ff7f00",  # orange
        'costa_rica_vms_reviewed':         "#6a3d9a",  # purple
        'ecuador_vms_reviewed':         "#e31a1c",  # red
        'ais_anchorage_overrides':  "#4B4B4B",  # gray
    }

    label_map = {
        'brazil_vms_reviewed':     'Brazil reviewed anchorages',
        'chile_vms_reviewed':      'Chile reviewed anchorages',
        'panama_vms_reviewed':        'Panama reviewed anchorages',
        'costa_rica_vms_reviewed':        'Costa Rica reviewed anchorages',
        'ecuador_vms_reviewed':        'Ecuador reviewed anchorages',
        'ais_anchorage_overrides': 'AIS anchorage overrides'
    }

    # Desired plotting order (from larger EEZs / regional grouping)
    plot_order = [
        'brazil_vms_reviewed',
        'chile_vms_reviewed',
        'ecuador_vms_reviewed',
        'panama_vms_reviewed',
        'costa_rica_vms_reviewed',
        'ais_anchorage_overrides'
    ]


    # Plot groups in controlled order
    for source in plot_order:
        group = gdf[gdf['source'] == source]
        if not group.empty:
            color = color_map.get(source, 'gray')
            label = label_map.get(source, source)
            group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

    # Plot any remaining sources not listed
    remaining_sources = [s for s in gdf['source'].unique() if s not in plot_order]
    for source in remaining_sources:
        group = gdf[gdf['source'] == source]
        color = color_map.get(source, 'gray')
        label = label_map.get(source, source)
        group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

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
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)  # good contrast

    # Tidy labels
    ax.set_title("Anchorage overrides (S2 centroids)")
    ax.set_axis_off()

    ax.legend()

    # Save high-res image
    plt.savefig(f"./figures/combined_anchorage_overrides_buffered_{focal_country_name}.png", dpi=300, bbox_inches="tight", facecolor="white")

    plt.show()

# %%


# %%
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import contextily as ctx

# 1) Build a GeoDataFrame from df['s2lat'], df['s2lon']
#    (drops rows with missing/invalid coords)
gdf = gpd.GeoDataFrame(
    target_anchorages,
    geometry=gpd.points_from_xy(target_anchorages['s2lon'], target_anchorages['s2lat']),
    crs="EPSG:4326"
)

# 2) Get Brazil polygon and bounds (Natural Earth included with GeoPandas)
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
brazil = world[world['name'] == 'Brazil'].to_crs(3857)  # Web Mercator for basemap

# 3) Project points to Web Mercator
gdf = gdf.to_crs(3857)

# Optional: pad the map extent a bit around Brazil
xmin, ymin, xmax, ymax = brazil.total_bounds
pad_x = (xmax - xmin) * 0.05
pad_y = (ymax - ymin) * 0.05
extent = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

# 4) Plot
fig, ax = plt.subplots(figsize=(10, 10))

# (Optional) outline Brazil for context
brazil.boundary.plot(ax=ax, linewidth=0.8, color="black", alpha=0.7)

color_map = {
    'ais_anchorage_overrides': "#FF0015",
    'brazil_vms_reviewed': "#002AB4",
    'chile_vms_reviewed': "#000619",
}

label_map = {
    'brazil_vms_reviewed': 'Brazil reviewed anchorages ',
    'chile_vms_reviewed': 'Chile anchorages',
    'ais_anchorage_overrides': 'AIS anchorage overrides'
}

# Define desired plotting order
plot_order = ['brazil_vms_reviewed', 'chile_vms_reviewed', 'ais_anchorage_overrides']


# Plot groups in controlled order
for source in plot_order:
    group = gdf[gdf['source'] == source]
    if not group.empty:
        color = color_map.get(source, 'gray')
        label = label_map.get(source, source)
        group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

# Plot any remaining sources not listed
remaining_sources = [s for s in gdf['source'].unique() if s not in plot_order]
for source in remaining_sources:
    group = gdf[gdf['source'] == source]
    color = color_map.get(source, 'gray')
    label = label_map.get(source, source)
    group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

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
ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)  # good contrast

# Tidy labels
ax.set_title("Anchorage overrides (S2 centroids)")
ax.set_axis_off()

ax.legend()

# Save high-res image
plt.savefig("./figures/combined_anchorage_overrides_brazil.png", dpi=300, bbox_inches="tight", facecolor="white")

plt.show()

# %%
# 1) Build a GeoDataFrame from df['s2lat'], df['s2lon']
#    (drops rows with missing/invalid coords)
gdf = gpd.GeoDataFrame(
    target_anchorages,
    geometry=gpd.points_from_xy(target_anchorages['s2lon'], target_anchorages['s2lat']),
    crs="EPSG:4326"
)

# 2) Get Brazil polygon and bounds (Natural Earth included with GeoPandas)
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
chile = world[world['name'] == 'Chile'].to_crs(3857)  # Web Mercator for basemap

# 3) Project points to Web Mercator
gdf = gdf.to_crs(3857)

# Optional: pad the map extent a bit around Brazil
xmin, ymin, xmax, ymax = chile.total_bounds
pad_x = (xmax - xmin) * 1.25
pad_y = (ymax - ymin) * 0.15
extent = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

# 4) Plot
fig, ax = plt.subplots(figsize=(10, 10))

# (Optional) outline Brazil for context
chile.boundary.plot(ax=ax, linewidth=0.8, color="black", alpha=0.7)

color_map = {
    'ais_anchorage_overrides': "#FF0015",
    'brazil_vms_reviewed': "#002AB4",
    'chile_vms_reviewed': "#000619"
}

label_map = {
    'brazil_vms_reviewed': 'Brazil reviewed anchorages ',
    'chile_vms_reviewed': 'Chile anchorages',
    'ais_anchorage_overrides': 'AIS anchorage overrides'
}

# Define desired plotting order
plot_order = ['brazil_vms_reviewed', 'chile_vms_reviewed', 'ais_anchorage_overrides']


# Plot groups in controlled order
for source in plot_order:
    group = gdf[gdf['source'] == source]
    if not group.empty:
        color = color_map.get(source, 'gray')
        label = label_map.get(source, source)
        group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

# Plot any remaining sources not listed
remaining_sources = [s for s in gdf['source'].unique() if s not in plot_order]
for source in remaining_sources:
    group = gdf[gdf['source'] == source]
    color = color_map.get(source, 'gray')
    label = label_map.get(source, source)
    group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

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
ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)  # good contrast

# Tidy labels
ax.set_title("Anchorage overrides (S2 centroids)")
ax.set_axis_off()

ax.legend()

# Save high-res image
plt.savefig("./figures/combined_anchorage_overrides_chile.png", dpi=300, bbox_inches="tight", facecolor="white")

plt.show()

# %% [markdown]
# ## scratch map

# %%


# Create a GeoDataFrame
gdf = gpd.GeoDataFrame(
    target_anchorages,
    geometry=gpd.points_from_xy(target_anchorages['lon'], target_anchorages['lat']),
    crs="EPSG:4326"
)

color_map = {
    'ais_anchorage_overrides': "#E84F5B",
    'brazil_vms_reviewed': "#02269E"
}

label_map = {
    'brazil_vms_reviewed': 'Brazil reviewed anchorages ',
    'ais_anchorage_overrides': 'AIS Anchorage Overrides'
}



# Create figure
fig, ax = plt.subplots(figsize=(10, 8))

# Define desired plotting order
plot_order = ['brazil_vms_reviewed', 'ais_anchorage_overrides']


# Plot groups in controlled order
for source in plot_order:
    group = gdf[gdf['source'] == source]
    if not group.empty:
        color = color_map.get(source, 'gray')
        label = label_map.get(source, source)
        group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

# Plot any remaining sources not listed
remaining_sources = [s for s in gdf['source'].unique() if s not in plot_order]
for source in remaining_sources:
    group = gdf[gdf['source'] == source]
    color = color_map.get(source, 'gray')
    label = label_map.get(source, source)
    group.plot(ax=ax, markersize=20, color=color, alpha=0.7, label=label)

# Add a basemap (optional)
ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, crs=gdf.crs.to_string())

ax.set_title("S2 cells with anchorage overrides")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.legend()
#plt.savefig('./figures/brazil_combined_anchorage_overrides.png', dpi=600)
plt.show()


# %%


# %% [markdown]
# # scratch - interactive maps

# %%
from s2sphere import CellId, Cell, LatLng
import folium

# Example data
tokens = list(target_anchorages['s2id'])
labels = list(target_anchorages['label'])

# Create map centered on first cell
first_cell = Cell(CellId.from_token(tokens[0]))
first_center = LatLng.from_point(first_cell.get_center())
m = folium.Map(location=[first_center.lat().degrees, first_center.lng().degrees], zoom_start=18)

for token, label in zip(tokens, labels):
    try:
      cell_id = CellId.from_token(token)
      cell = Cell(cell_id)

      # Polygon coordinates (lat, lon)
      poly = []
      for i in range(4):
          v = cell.get_vertex(i)
          ll = LatLng.from_point(v)
          poly.append([ll.lat().degrees, ll.lng().degrees])
      poly.append(poly[0])

      # Cell center
      center_ll = LatLng.from_point(cell.get_center())
      center = [center_ll.lat().degrees, center_ll.lng().degrees]

      # Draw polygon
      folium.Polygon(
          poly,
          color="red",
          weight=2,
          fill=True,
          fill_opacity=0.2
      ).add_to(m)

      # Add label exactly at center
      folium.Marker(
          location=center,
          icon=folium.DivIcon(
              html=f'''
              <div style="
                  display: inline-block;
                  font-size: 13px;
                  font-weight: bold;
                  color: black;
                  background-color: rgba(255, 255, 255, 0.9);
                  padding: 4px 8px;
                  border-radius: 4px;
                  text-align: center;
                  transform: translate(-50%, -50%); /* centers div over marker */
                  position: relative;
                  min-width: 50px;
                  white-space: nowrap;
                  box-shadow: 0 0 2px rgba(0,0,0,0.2);
              ">
                  {label}
              </div>
              '''
          )
      ).add_to(m)
    except:
      print(token)

m

# %%
target_anchorages

# %%
from s2sphere import CellId, Cell, LatLng
import folium
from folium.features import GeoJson
import pandas as pd

# --- prep target df ---
target_anchorages = combined_anchorages[combined_anchorages['iso3'] == 'ECU'].reset_index(drop=True)
target_anchorages['s2lat'], target_anchorages['s2lon'] = zip(*target_anchorages['s2id'].map(s2id_to_latlon))

# ---- build a color map per source ----
palette = [
    "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
    "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf"
]
sources = pd.Series(target_anchorages['source']).fillna('UNKNOWN')
unique_sources = list(sources.unique())
source_color = {src: palette[i % len(palette)] for i, src in enumerate(unique_sources)}

def color_for(src):
    return source_color.get(src if pd.notna(src) else 'UNKNOWN', "#555555")

# center on first cell
first_cell = Cell(CellId.from_token(target_anchorages['s2id'].iloc[0]))
first_center = LatLng.from_point(first_cell.get_center())
m = folium.Map(
    location=[first_center.lat().degrees, first_center.lng().degrees],
    zoom_start=18,
    prefer_canvas=True,
)

# ---- build GeoJSON FeatureCollection (now includes 'source') ----
features = []
for _, row in target_anchorages.iterrows():
    token = row['s2id']
    label = row.get('label', 'NULL')
    sublabel = row.get('sublabel', 'NULL')
    src = row.get('source', 'UNKNOWN')

    try:
        cell = Cell(CellId.from_token(token))
    except Exception:
        print(f"Invalid token: {token}")
        continue

    ring = []
    for i in range(4):
        v = cell.get_vertex(i)
        ll = LatLng.from_point(v)
        ring.append([ll.lng().degrees, ll.lat().degrees])
    ring.append(ring[0])

    features.append({
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "properties": {
            "label_html": f"Label: {label}<br>Sublabel: {sublabel}",
            "source": 'UNKNOWN' if pd.isna(src) else src
        }
    })

fc = {"type": "FeatureCollection", "features": features, "tolerance": 0}

# ---- style by feature['properties']['source'] ----
gj = GeoJson(
    fc,
    name="S2 Cells",
    style_function=lambda feat: {
        "color": color_for(feat["properties"].get("source", "UNKNOWN")),
        "weight": 2,
        "fill": True,
        "fillColor": color_for(feat["properties"].get("source", "UNKNOWN")),
        "fillOpacity": 0.2,
    },
    # uncomment if you want hover tooltips from label_html
    # tooltip=folium.GeoJsonTooltip(fields=[], aliases=[], labels=False, sticky=False)
)
gj.add_to(m)

# ---- centroid points colored by source ----
centroid_layer = folium.FeatureGroup(name='S2 Centroid')
for _, row in target_anchorages.iterrows():
    src = row.get('source', 'UNKNOWN')
    folium.CircleMarker(
        location=[row['s2lat'], row['s2lon']],
        radius=2,
        color=color_for(src),
        fill=True,
        fill_opacity=0.7,
        popup=f"Lat: {row['s2lat']}, Lon: {row['s2lon']}<br>Source: {src}"
    ).add_to(centroid_layer)
centroid_layer.add_to(m)

# ---- label markers (unchanged) ----
high_zoom_labels = folium.FeatureGroup(name="Label / Sublabel")
for _, row in target_anchorages.iterrows():
    sublabel = row.get("sublabel", "NULL")
    label = row.get("label", "NULL")
    center = [row["s2lat"], row["s2lon"]]
    folium.Marker(
        location=center,
        icon=folium.DivIcon(
            html=f'''
            <div style="
                display:inline-block;font-size:15px;color:black;
                background-color:rgba(255,255,255,0.9);padding:4px 8px;border-radius:4px;
                text-align:center;transform:translate(-50%,-50%);position:relative;
                min-width:50px;white-space:nowrap;box-shadow:0 0 2px rgba(0,0,0,0.2);
            ">{label}<br>{sublabel}</div>
            '''
        )
    ).add_to(high_zoom_labels)
high_zoom_labels.add_to(m)

# ---- add a legend for sources ----
legend_html = """
<div style="
 position: fixed; bottom: 20px; left: 20px; z-index: 9999;
 background: white; padding: 8px 10px; border: 1px solid #ccc; border-radius: 4px;
 font-size: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.2);">
 <b>Source</b><br>
"""
for src in unique_sources:
    legend_html += f'<span style="display:inline-block;width:10px;height:10px;background:{source_color[src]};margin-right:6px;border:1px solid #999;"></span>{src}<br>'
legend_html += "</div>"
m.get_root().html.add_child(folium.Element(legend_html))

folium.LayerControl(collapsed=False).add_to(m)
m

# %%
target_anchorages

# %%
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import contextily as ctx  # for basemaps (optional)
from s2sphere import CellId, LatLng


# %%
token

# %%
for idx, row in ais_anchorages.iterrows():
    s2id = str(row['s2id'])
    if '+' in s2id:
        print(f"Row {idx}: {s2id}")
        break

# %%
for idx, row in ais_anchorages.iterrows():
    s2id = str(row['s2id'])
    if '+' in s2id:
        print(f"Row {idx}: {s2id}")
        break

# %%


# %%
idx

# %%
ais_anchorages

# %%


# %%
ais_anchorages.iloc[idx-2]

# %%


# %% [markdown]
# # map of named anchorages - by source

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

# %%


# %% [markdown]
# # map of named anchorages

# %%
q = f'''
SELECT
  *
FROM
  `world-fishing-827.scratch_amanda_ttl_120.combined_named_anchorages_v20251017`
'''
df = get_bq_df(q)
df['s2lat'], df['s2lon'] = zip(*df['s2id'].map(s2id_to_latlon))
df

# %%
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import contextily as ctx

# 1) Build a GeoDataFrame from df['s2lat'], df['s2lon']
#    (drops rows with missing/invalid coords)
coords = df[['s2lat','s2lon']].dropna().copy()
gdf = gpd.GeoDataFrame(
    coords,
    geometry=gpd.points_from_xy(coords['s2lon'], coords['s2lat']),
    crs="EPSG:4326"
)

# 2) Get Brazil polygon and bounds (Natural Earth included with GeoPandas)
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
brazil = world[world['name'] == 'Brazil'].to_crs(3857)  # Web Mercator for basemap

# 3) Project points to Web Mercator
gdf_3857 = gdf.to_crs(3857)

# Optional: pad the map extent a bit around Brazil
xmin, ymin, xmax, ymax = brazil.total_bounds
pad_x = (xmax - xmin) * 0.05
pad_y = (ymax - ymin) * 0.05
extent = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

# 4) Plot
fig, ax = plt.subplots(figsize=(10, 10))

# (Optional) outline Brazil for context
brazil.boundary.plot(ax=ax, linewidth=0.8, color="black", alpha=0.7)

# Plot points (choose a high-contrast, colorblind-friendly color)
gdf_3857.plot(
    ax=ax,
    markersize=15,
    color="#0072B2",      # blue (CUD palette)
    alpha=0.85,
    edgecolor="white",
    linewidth=0.2
)



# Zoom to Brazil
ax.set_xlim(extent[0], extent[1])
ax.set_ylim(extent[2], extent[3])

# Add a clean basemap
ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)  # good contrast

# Tidy labels
ax.set_title("All anchorages (S2 centroids)")
ax.set_axis_off()


# Save high-res image
plt.savefig("./figures/all_anchorages_brazil.png", dpi=300, bbox_inches="tight", facecolor="white")
plt.show()

# %%
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import contextily as ctx

# 1) Build a GeoDataFrame from df['s2lat'], df['s2lon']
#    (drops rows with missing/invalid coords)
coords = df[['s2lat','s2lon']].dropna().copy()
gdf = gpd.GeoDataFrame(
    coords,
    geometry=gpd.points_from_xy(coords['s2lon'], coords['s2lat']),
    crs="EPSG:4326"
)

# 2) Get Brazil polygon and bounds (Natural Earth included with GeoPandas)
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
focal_country = world[world['name'] == 'Chile'].to_crs(3857)  # Web Mercator for basemap

# 3) Project points to Web Mercator
gdf_3857 = gdf.to_crs(3857)

# Optional: pad the map extent a bit around Brazil
xmin, ymin, xmax, ymax = focal_country.total_bounds
pad_x = (xmax - xmin) * 1.25
pad_y = (ymax - ymin) * 0.15
extent = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

# 4) Plot
fig, ax = plt.subplots(figsize=(10, 10))

# (Optional) outline Brazil for context
focal_country.boundary.plot(ax=ax, linewidth=0.8, color="black", alpha=0.7)

# Plot points (choose a high-contrast, colorblind-friendly color)
gdf_3857.plot(
    ax=ax,
    markersize=15,
    color="#0072B2",      # blue (CUD palette)
    alpha=0.85,
    edgecolor="white",
    linewidth=0.2
)



# Zoom to focal_country
ax.set_xlim(extent[0], extent[1])
ax.set_ylim(extent[2], extent[3])

# Add a clean basemap
ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)  # good contrast

# Tidy labels
ax.set_title("All anchorages (S2 centroids)")
ax.set_axis_off()


# Save high-res image
plt.savefig("./figures/all_anchorages_chile.png", dpi=300, bbox_inches="tight", facecolor="white")
plt.show()

# %%


# %% [markdown]
# # s2 interactive - all anchorages

# %%
q = f'''
SELECT
  *
FROM
  `world-fishing-827.scratch_amanda_ttl_120.combined_named_anchorages_precursor_v20251024`
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
country_names = ['costa_rica','ecuador','panama','chile']

for country_name in country_names:
    df.loc[df["source"] == f"{country_name}_vms_reviewed", "source"] = f"{country_name}_vms_reviewed_buffer"
    df1 = pd.read_csv(f'../../pipe_anchorages/data/port_lists/{country_name}_singleS2cell_overrides.csv')
    df.loc[df["s2id"].isin(df1["s2id"]), "source"] = f"{country_name}_vms_reviewed_provided_point"

np.unique(df['source'])

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
m.save(f"{fig_fldr}/{country_name}_comined_named_anchorages.html")
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
m.save(f"{fig_fldr}/{country_name}_comined_named_anchorages.html")
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
m.save(f"{fig_fldr}/{country_name}_comined_named_anchorages.html")
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
m.save(f"{fig_fldr}/{country_name}_comined_named_anchorages.html")
m

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
m.save(f"{fig_fldr}/{country_name}_comined_named_anchorages.html")
m

# %%


# %%


# %%
from s2sphere import CellId, Cell, LatLng
import folium
from folium.features import GeoJson
import pandas as pd

# --- prep target df ---
target_anchorages = df[df['iso3'] == 'ECU'].reset_index(drop=True)
target_anchorages['s2lat'], target_anchorages['s2lon'] = zip(*target_anchorages['s2id'].map(s2id_to_latlon))

# ---- build a color map per source ----
palette = [
    "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
    "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf"
]

# Ensure 'ais_detected' always exists as a source
sources = pd.Series(target_anchorages['source']).fillna('ais_detected')
unique_sources = list(sources.unique())
if "ais_detected" not in unique_sources:
    unique_sources.append("ais_detected")

# Assign colors by cycling through the palette
source_color = {src: palette[i % len(palette)] for i, src in enumerate(unique_sources)}

def color_for(src):
    if pd.isna(src):
        return source_color["ais_detected"]
    return source_color.get(src, source_color["ais_detected"])

# center on first cell
first_cell = Cell(CellId.from_token(target_anchorages['s2id'].iloc[0]))
first_center = LatLng.from_point(first_cell.get_center())
m = folium.Map(
    location=[first_center.lat().degrees, first_center.lng().degrees],
    zoom_start=18,
    prefer_canvas=True,
)

# ---- build GeoJSON FeatureCollection (with source property) ----
features = []
for _, row in target_anchorages.iterrows():
    token = row['s2id']
    label = row.get('label', 'NULL')
    sublabel = row.get('sublabel', 'NULL')
    src = row.get('source', 'ais_detected')

    try:
        cell = Cell(CellId.from_token(token))
    except Exception:
        print(f"Invalid token: {token}")
        continue

    ring = []
    for i in range(4):
        v = cell.get_vertex(i)
        ll = LatLng.from_point(v)
        ring.append([ll.lng().degrees, ll.lat().degrees])
    ring.append(ring[0])

    features.append({
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "properties": {
            "label_html": f"Label: {label}<br>Sublabel: {sublabel}",
            "source": src if pd.notna(src) else "ais_detected"
        }
    })

fc = {"type": "FeatureCollection", "features": features, "tolerance": 0}

# ---- style by feature['properties']['source'] ----
gj = GeoJson(
    fc,
    name="S2 Cells",
    style_function=lambda feat: {
        "color": color_for(feat["properties"].get("source", "ais_detected")),
        "weight": 2,
        "fill": True,
        "fillColor": color_for(feat["properties"].get("source", "ais_detected")),
        "fillOpacity": 0.2,
    },
)
gj.add_to(m)

# ---- centroid points colored by source ----
centroid_layer = folium.FeatureGroup(name='S2 Centroid')
for _, row in target_anchorages.iterrows():
    src = row.get('source', 'ais_detected')
    folium.CircleMarker(
        location=[row['s2lat'], row['s2lon']],
        radius=2,
        color=color_for(src),
        fill=True,
        fill_opacity=0.7,
        popup=f"Lat: {row['s2lat']}, Lon: {row['s2lon']}<br>Source: {src}"
    ).add_to(centroid_layer)
centroid_layer.add_to(m)

# ---- label markers (unchanged) ----
high_zoom_labels = folium.FeatureGroup(name="Label / Sublabel")
for _, row in target_anchorages.iterrows():
    sublabel = row.get("sublabel", "NULL")
    label = row.get("label", "NULL")
    center = [row["s2lat"], row["s2lon"]]
    folium.Marker(
        location=center,
        icon=folium.DivIcon(
            html=f'''
            <div style="
                display:inline-block;font-size:15px;color:black;
                background-color:rgba(255,255,255,0.9);padding:4px 8px;border-radius:4px;
                text-align:center;transform:translate(-50%,-50%);position:relative;
                min-width:50px;white-space:nowrap;box-shadow:0 0 2px rgba(0,0,0,0.2);
            ">{label}<br>{sublabel}</div>
            '''
        )
    ).add_to(high_zoom_labels)
high_zoom_labels.add_to(m)

# ---- add a legend for sources ----
legend_html = """
<div style="
 position: fixed; bottom: 20px; left: 20px; z-index: 9999;
 background: white; padding: 8px 10px; border: 1px solid #ccc; border-radius: 4px;
 font-size: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.2);">
 <b>Source</b><br>
"""

# Define friendly labels for specific sources
legend_labels = {
    "ais_anchorage_overrides": "AIS anchorage_overrides file",
    "ecuador_vms_reviewed": "Ecuador reviewed anchorages",
    "costa_rica_vms_reviewed": "Costa Rica reviewed anchorages",
     "brazil_vms_reviewed":"Brazil reviewed anchorages",
    "ais_detected": "Detected from AIS data",
}

# Define custom display order
legend_order = [
    "ais_anchorage_overrides",
    "ecuador_vms_reviewed",
    "costa_rica_vms_reviewed",
    "brazil_vms_reviewed",
    "ais_detected",
]

# Build legend entries in the specified order
for src in legend_order:
    if src in source_color:  # only include if it exists in your color map
        legend_label = legend_labels.get(src, src)
        legend_html += (
            f'<span style="display:inline-block;width:10px;height:10px;'
            f'background:{source_color[src]};margin-right:6px;border:1px solid #999;"></span>'
            f'{legend_label}<br>'
        )

legend_html += "</div>"
m.get_root().html.add_child(folium.Element(legend_html))

folium.LayerControl(collapsed=False).add_to(m)
m

# %% [markdown]
# # CIV examination

# %%
df = pd.read_csv('../../pipe_anchorages/data/port_lists/anchorage_overrides.csv')
df1 = df[df['sublabel']=='ABIDJAN OFFSHORE']
df1

# %%
df2 = df[df['s2id'].isin(df1['s2id'])]
df2 = df2[df2['sublabel'] != 'ABIDJAN OFFSHORE']
df2



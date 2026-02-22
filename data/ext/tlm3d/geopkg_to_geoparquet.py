import fiona
import geopandas as gpd
from pathlib import Path

gpkg = Path("SWISSTLM3D_2025.gpkg")

#for layer in fiona.listlayers(gpkg):  # discover all layers[web:56]
for layer in ["tlm_oev_eisenbahn", "tlm_oev_haltestelle"]:
    print(f"processing {layer}")
    gdf = gpd.read_file(gpkg, layer=layer)
    gdf.to_parquet(f"{gpkg.stem}_{layer}.geoparquet", index=False)

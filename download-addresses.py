#!/usr/bin/env python3


"""Download all addresses in Austria."""


import datetime
import itertools
import pathlib
import tempfile
import time
import zipfile

import geopandas
import overpy
import shapely

# Download per NUTS-3 area, to split the country into smaller portions
NUTS_AREAS = [
    "AT111",
    "AT112",
    "AT113",
    "AT121",
    "AT122",
    "AT123",
    "AT124",
    "AT125",
    "AT126",
    "AT127",
    "AT130",
    "AT211",
    "AT212",
    "AT213",
    "AT221",
    "AT222",
    "AT223",
    "AT224",
    "AT225",
    "AT226",
    "AT311",
    "AT312",
    "AT313",
    "AT314",
    "AT315",
    "AT321",
    "AT322",
    "AT323",
    "AT331",
    "AT332",
    "AT333",
    "AT334",
    "AT335",
    "AT341",
    "AT342",
]
CLIP_TO_ADM0 = "Österreich"
OUTPUT_FILENAME = pathlib.Path(__file__).parent / "austrian-addresses.gpkg"
WAITING_TIME = datetime.timedelta(minutes=3).total_seconds()


def main():
    addresses = {
        "street": [],
        "housenumber": [],
        "postcode": [],
        "city": [],
        "geometry": [],
    }

    api = overpy.Overpass()
    results = None
    while results is None:
        try:
            results = api.query(
                "[out:json][timeout:25];"
                f'rel["name"="{CLIP_TO_ADM0}"]["boundary"="administrative"];'
                "out geom;"
            )
        except (
            overpy.exception.OverpassGatewayTimeout,
            overpy.exception.OverpassTooManyRequests,
        ):
            time.sleep(WAITING_TIME)
    clip_polygon = (
        geopandas.GeoDataFrame(
            {
                "geometry": [
                    shapely.unary_union(
                        shapely.polygonize(
                            [
                                shapely.unary_union(
                                    shapely.line_merge(
                                        [
                                            shapely.LineString(
                                                [
                                                    (
                                                        float(coords.lon),
                                                        float(coords.lat),
                                                    )
                                                    for coords in member.geometry
                                                ]
                                            )
                                            for relation in results.relations
                                            for member in relation.members
                                            if member.role == "outer"
                                            and member.geometry is not None
                                        ]
                                    )
                                )
                            ]
                        )
                    )
                ]
            },
            crs="EPSG:4326",
        )
        .to_crs("EPSG:31287")
        .buffer(2000.0)
        .simplify(2000.0)
        .to_crs("EPSG:4326")
    )

    for area in NUTS_AREAS:
        results = None
        while results is None:
            try:
                results = api.query(
                    "[out:json][timeout:3600];"
                    f'area["ref:nuts:3"="{area}"];'
                    'nwr["addr:housenumber"](area);'
                    "out center;"
                )
            except (
                overpy.exception.OverpassGatewayTimeout,
                overpy.exception.OverpassTooManyRequests,
            ):
                time.sleep(WAITING_TIME)

        for element in itertools.chain(results.nodes, results.ways, results.relations):
            for tag in [
                "street",
                "housenumber",
                "postcode",
                "city",
            ]:
                try:
                    addresses[tag].append(element.tags[f"addr:{tag}"])
                except KeyError:
                    addresses[tag].append(None)
            try:
                geometry = shapely.Point(element.center_lon, element.center_lat)
            except AttributeError:
                try:
                    geometry = shapely.Point(element.lon, element.lat)
                except:
                    # print("no geom?")
                    geometry = shapely.Point()
            addresses["geometry"].append(geometry)

    df = geopandas.GeoDataFrame(addresses, crs="EPSG:4326")
    df["id"] = df.index

    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_directory = pathlib.Path(temporary_directory)
        df.to_file(temporary_directory / OUTPUT_FILENAME.name)
        with zipfile.ZipFile(
            OUTPUT_FILENAME.with_suffix(f"{OUTPUT_FILENAME.suffix}.zip"),
            "w",
            zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.write(
                temporary_directory / OUTPUT_FILENAME.name, OUTPUT_FILENAME.name
            )

    voronoi_polygons = geopandas.GeoDataFrame(
        {
            "geometry": df.geometry.voronoi_polygons(),
        }
    )

    voronoi_polygons = voronoi_polygons.sjoin(
        df,
        how="left",
        predicate="contains",
        lsuffix="",
    )
    voronoi_polygons["id"] = voronoi_polygons.index
    voronoi_polygons = voronoi_polygons[
        [
            "id",
            "street",
            "housenumber",
            "postcode",
            "city",
            "geometry",
        ]
    ]
    voronoi_polygons = voronoi_polygons.clip(clip_polygon, keep_geom_type=True)

    output_filename = (
        OUTPUT_FILENAME.parent
        / f"{OUTPUT_FILENAME.stem}-voronoi{OUTPUT_FILENAME.suffix}"
    )
    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_directory = pathlib.Path(temporary_directory)
        voronoi_polygons.to_file(temporary_directory / output_filename.name)
        with zipfile.ZipFile(
            output_filename.with_suffix(f"{output_filename.suffix}.zip"),
            "w",
            zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.write(
                temporary_directory / output_filename.name, output_filename.name
            )


if __name__ == "__main__":
    main()

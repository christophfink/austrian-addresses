#!/usr/bin/env python3


"""Download all addresses in Austria."""


import datetime
import itertools
import pathlib
import re
import tempfile
import time
import zipfile

import geopandas
import overpy
import pandas
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
POSTCODE_NOTE_RE = re.compile("(?P<post_code>[0-9]{4}) (?P<city>.*)$")


def download_clip_polygon():
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
    return clip_polygon


def download_postcode_areas():
    postcode_areas = {
        "city": [],
        "postcode": [],
        "geometry": [],
    }

    api = overpy.Overpass()
    for area in NUTS_AREAS:
        results = None
        while results is None:
            try:
                results = api.query(
                    "[out:json][timeout:3600];"
                    f'area["ref:nuts:3"="{area}"];'
                    'rel(area)["boundary"="postal_code"];'
                    "out geom;"
                )
            except (
                overpy.exception.OverpassGatewayTimeout,
                overpy.exception.OverpassTooManyRequests,
            ):
                time.sleep(WAITING_TIME)

        for element in results.relations:
            try:
                postcode = element.tags["postal_code"]
            except KeyError:
                postcode = None
            try:
                notes = POSTCODE_NOTE_RE.match(element.tags["note"])
                city = notes["city"]
            except (KeyError, TypeError):
                city = None

            geometry = shapely.unary_union(
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
                                    for member in element.members
                                    if member.role == "outer"
                                    and member.geometry is not None
                                ]
                            )
                        )
                    ]
                )
            )

            postcode_areas["postcode"].append(postcode)
            postcode_areas["city"].append(city)
            postcode_areas["geometry"].append(geometry)

    postcode_areas = geopandas.GeoDataFrame(postcode_areas, crs="EPSG:4326")
    postcode_areas["geometry"] = postcode_areas["geometry"].normalize()
    postcode_areas = postcode_areas.drop_duplicates()

    return postcode_areas


def download_municipalities():
    municipalities = {
        "city": [],
        "geometry": [],
    }

    api = overpy.Overpass()
    for area in NUTS_AREAS:
        results = None
        while results is None:
            try:
                results = api.query(
                    "[out:json][timeout:3600];"
                    f'area["ref:nuts:3"="{area}"];'
                    'rel(area)["boundary"="administrative"]["admin_level"="8"];'
                    "out geom;"
                )
            except (
                overpy.exception.OverpassGatewayTimeout,
                overpy.exception.OverpassTooManyRequests,
            ):
                time.sleep(WAITING_TIME)

        for element in results.relations:
            city = element.tags["name"]

            geometry = shapely.unary_union(
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
                                    for member in element.members
                                    if member.role == "outer"
                                    and member.geometry is not None
                                ]
                            )
                        )
                    ]
                )
            )

            municipalities["city"].append(city)
            municipalities["geometry"].append(geometry)

    municipalities = geopandas.GeoDataFrame(municipalities, crs="EPSG:4326")
    municipalities["geometry"] = municipalities["geometry"].normalize()
    municipalities = municipalities.drop_duplicates()

    return municipalities


def download_housenumbers(clip_polygon, postcode_areas, municipalities):
    addresses = {
        "street": [],
        "housenumber": [],
        "postcode": [],
        "city": [],
        "geometry": [],
    }

    api = overpy.Overpass()
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

        for element in itertools.chain(
            results.nodes,
            results.ways,
            results.relations,
        ):
            try:
                geometry = shapely.Point(
                    element.center_lon,
                    element.center_lat,
                )
            except AttributeError:
                try:
                    geometry = shapely.Point(element.lon, element.lat)
                except:  # noqa: E722
                    # print("no geom?")
                    geometry = shapely.Point()
            addresses["geometry"].append(geometry)

            for tag in [
                "street",
                "housenumber",
                "postcode",
                "city",
            ]:
                try:
                    addresses[tag].append(element.tags[f"addr:{tag}"])
                except KeyError:
                    if tag in ["postcode", "city"]:
                        try:
                            postcode_area = postcode_areas.loc[
                                postcode_areas.sindex.query(
                                    geometry,
                                    predicate="within",
                                )
                            ][0]
                            addresses[tag] = postcode_area[tag]
                        except (IndexError, KeyError):
                            if tag == "city":
                                try:
                                    municipality = municipalities.loc[
                                        municipalities.sindex.query(
                                            geometry,
                                            predicate="within",
                                        )
                                    ][0]
                                    addresses[tag] = municipality["name"]
                                except (IndexError, KeyError):
                                    addresses[tag].append(None)
                            else:
                                addresses[tag].append(None)
                    else:
                        addresses[tag].append(None)

    addresses["postcode"] = addresses["postcode"].astype("int")

    addresses = geopandas.GeoDataFrame(addresses, crs="EPSG:4326")
    addresses["geometry"] = addresses.normalize()
    addresses = addresses.drop_duplicates(["geometry"])
    addresses = addresses.drop_duplicates(["street", "housenumber", "postcode", "city"])


def fill_in_gaps(addresses):
    # 1) set city from other records with the same postcode
    postcodes = (
        addresses[addresses.city.notnull()]
        .groupby("postcode")
        .first()
        .reset_index()[["postcode", "city"]]
        .set_index("postcode")
    )
    addresses = (
        addresses.set_index("postcode")
        .join(postcodes["city"], rsuffix="_from_postcodes")
        .reset_index()
    )
    addresses.loc[
        addresses.city.isna() & addresses.city_from_postcodes.notnull(),
        "city",
    ] = addresses["city_from_postcodes"]

    # reset columns
    addresses = addresses[
        [
            "street",
            "housenumber",
            "postcode",
            "city",
            "geometry",
        ]
    ]

    # 2) copy the most common values from neighbouring polygons
    addresses = addresses.reset_index(names="id")
    neighbours = (
        addresses[addresses.city.isna() | addresses.postcode.isna()]
        .sjoin(
            addresses[
                [
                    "postcode",
                    "city",
                    "geometry",
                ]
            ],
            how="left",
            predicate="touches",
        )
        .groupby("id", as_index=False)[["postcode_right", "city_right"]]
        .agg(pandas.Series.mode)
        .set_index("id")
    )
    addresses = addresses.join(neighbours)
    addresses.loc[addresses.postcode.isna(), "postcode"] = addresses["postcode_right"]
    addresses.loc[addresses.city.isna(), "city"] = addresses["city_right"]

    # reset columns
    addresses = addresses[
        [
            "street",
            "housenumber",
            "postcode",
            "city",
            "geometry",
        ]
    ]

    # 3) TODO: fill in street names (maybe)

    return addresses


def main():
    clip_polygon = download_clip_polygon()
    postcode_areas = download_postcode_areas()
    municipalities = download_municipalities()
    addresses = download_housenumbers(
        clip_polygon,
        postcode_areas,
        municipalities,
    )

    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_directory = pathlib.Path(temporary_directory)
        addresses.to_file(temporary_directory / OUTPUT_FILENAME.name)
        with zipfile.ZipFile(
            OUTPUT_FILENAME.with_suffix(f"{OUTPUT_FILENAME.suffix}.zip"),
            "w",
            zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.write(
                temporary_directory / OUTPUT_FILENAME.name,
                OUTPUT_FILENAME.name,
            )

    voronoi_polygons = geopandas.GeoDataFrame(
        {
            "geometry": addresses.geometry.voronoi_polygons(),
        }
    )

    voronoi_polygons = voronoi_polygons.sjoin(
        addresses,
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

    voronoi_polygons = fill_in_gaps(voronoi_polygons)

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
                temporary_directory / output_filename.name,
                output_filename.name,
            )


if __name__ == "__main__":
    main()

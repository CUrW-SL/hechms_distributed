from resources import manager as res_mgr
from input.shape_util.spatial_util import get_voronoi_polygons
import pandas as pd
import os
import copy
import datetime
import numpy as np
import geopandas as gpd
from scipy.spatial import Voronoi
from input.station_metadata import meta_data
from db_layer import get_event_id, get_time_series_values
from config import SUB_CATCHMENT_SHAPE_FILE_DIR, THESSIAN_DECIMAL_POINTS
from db_layer import MySqlAdapter


TIME_GAP_MINUTES = 5
MISSING_ERROR_PERCENTAGE = 0.1


def _voronoi_finite_polygons_2d(vor, radius=None):
    """
    Reconstruct infinite voronoi regions in a 2D diagram to finite
    regions.

    Parameters
    ----------
    vor : Voronoi
        Input diagram
    radius : float, optional
        Distance to 'points at infinity'.

    Returns
    -------
    regions : list of tuples
        Indices of vertices in each revised Voronoi regions.
    vertices : list of tuples
        Coordinates for revised Voronoi vertices. Same as coordinates
        of input vertices, with 'points at infinity' appended to the
        end.

    from: https://stackoverflow.com/questions/20515554/colorize-voronoi-diagram

    """

    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D input")

    new_regions = []
    new_vertices = vor.vertices.tolist()

    center = vor.points.mean(axis=0)
    if radius is None:
        radius = vor.points.ptp().max()

    # Construct a map containing all ridges for a given point
    all_ridges = {}
    for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
        all_ridges.setdefault(p1, []).append((p2, v1, v2))
        all_ridges.setdefault(p2, []).append((p1, v1, v2))

    # Reconstruct infinite regions
    for p1, region in enumerate(vor.point_region):
        vertices = vor.regions[region]

        if all(v >= 0 for v in vertices):
            # finite region
            new_regions.append(vertices)
            continue

        # reconstruct a non-finite region
        ridges = all_ridges[p1]
        new_region = [v for v in vertices if v >= 0]

        for p2, v1, v2 in ridges:
            if v2 < 0:
                v1, v2 = v2, v1
            if v1 >= 0:
                # finite ridge: already in the region
                continue

            # Compute the missing endpoint of an infinite ridge

            t = vor.points[p2] - vor.points[p1]  # tangent
            t /= np.linalg.norm(t)
            n = np.array([-t[1], t[0]])  # normal

            midpoint = vor.points[[p1, p2]].mean(axis=0)
            direction = np.sign(np.dot(midpoint - center, n)) * n
            far_point = vor.vertices[v2] + direction * radius

            new_region.append(len(new_vertices))
            new_vertices.append(far_point.tolist())

        # sort region counterclockwise
        vs = np.asarray([new_vertices[v] for v in new_region])
        c = vs.mean(axis=0)
        angles = np.arctan2(vs[:, 1] - c[1], vs[:, 0] - c[0])
        new_region = np.array(new_region)[np.argsort(angles)]

        # finish
        new_regions.append(new_region.tolist())
    return new_regions, np.asarray(new_vertices)


def get_gage_points():
    gage_csv = res_mgr.get_resource_path('gages/CurwRainGauges.csv')
    gage_df = pd.read_csv(gage_csv)[['name', 'longitude', 'latitude']]
    gage_dict = gage_df.set_index('name').T.to_dict('list')
    return gage_dict


def get_thessian_polygon_from_gage_points(shape_file, gage_points):
    shape = res_mgr.get_resource_path(shape_file)
    # calculate the voronoi/thesian polygons w.r.t given station points.
    thessian_df = get_voronoi_polygons(gage_points, shape, ['OBJECTID', 1],
                                  output_shape_file=os.path.join(SUB_CATCHMENT_SHAPE_FILE_DIR, 'sub_catchment.shp'))
    return thessian_df


def get_catchment_area(catchment_file):
    shape = res_mgr.get_resource_path(catchment_file)
    catchment_df = gpd.GeoDataFrame.from_file(shape)
    return catchment_df


def calculate_intersection(thessian_df, catchment_df):
    sub_ratios = []
    for i, catchment_polygon in enumerate(catchment_df['geometry']):
        sub_catchment_name = catchment_df.iloc[i]['Name_of_Su']
        ratio_list = []
        for j, thessian_polygon in enumerate(thessian_df['geometry']):
            if catchment_polygon.intersects(thessian_polygon):
                gage_name = thessian_df.iloc[j]['id']
                intersection = catchment_polygon.intersection(thessian_polygon)
                ratio = np.round(intersection.area / thessian_polygon.area, THESSIAN_DECIMAL_POINTS)
                ratio_dic = {'gage_name': gage_name, 'ratio': ratio}
                ratio_list.append(ratio_dic)
        #print('')
        sub_dict = {'sub_catchment_name': sub_catchment_name, 'ratios': ratio_list}
        sub_ratios.append(sub_dict)
        #print(sub_dict)
    return sub_ratios


def get_sub_catchment_rainfall(data_from, data_to, db_adapter, sub_dict, station_metadata=meta_data):
    stations_meta = copy.deepcopy(station_metadata)
    sub_catchment_name = sub_dict['sub_catchment_name']
    print('sub_catchment_name:',sub_catchment_name)
    ratio_list = sub_dict['ratios']
    for gage_dict in ratio_list:
        print('gage_dict:',gage_dict)
        gage_name = gage_dict['gage_name']
        timeseries_meta = stations_meta[gage_name]
        print('timeseries_meta:',timeseries_meta)
        try:
            db_meta_data = {'station': gage_name,
                            'variable': timeseries_meta['variable'],
                            'unit': timeseries_meta['unit'],
                            'type': timeseries_meta['event_type'],
                            'source': timeseries_meta['source'],
                            'name': timeseries_meta['run_name']}
            event_id = get_event_id(db_adapter, db_meta_data)
            print('event_id:', event_id)
            time_series_df = get_time_series_values(db_adapter, event_id, data_from, data_to)
            ratio = gage_dict['ratio']
            print('ratio:', ratio)
            time_series_df.loc[:, 'value'] *= ratio
            print('time_series_df:', time_series_df)

        except Exception as e:
            print("get_event_id|Exception|e : ", e)


def get_kub_points_from_meta_data(station_metadata=meta_data):
    kub_points = {}
    print('station_metadata : ', type(station_metadata))
    for key, value in station_metadata.items():
        print('key : ', key)
        print('value : ', value)
        kub_points[key] = value['lon_lat']
    print('kub_points : ', kub_points)
    return kub_points


def validate_gage_points(db_adapter, datetime_from, datetime_to, station_metadata=meta_data):
    validated_gages = {}
    ts_datetime_from = datetime.datetime.strptime(datetime_from, '%Y-%m-%d %H:%M:%S')
    ts_datetime_to = datetime.datetime.strptime(datetime_to, '%Y-%m-%d %H:%M:%S')
    #print('days : ', (ts_datetime_to - ts_datetime_from).days)
    time_series_count = ((ts_datetime_to - ts_datetime_from).days * 24 * 60)/TIME_GAP_MINUTES
    #print('time_series_count : ', time_series_count)
    for key, value in station_metadata.items():
        #print('key : ', key)
        #print('value : ', value)
        try:
            db_meta_data = {'station': key,
                            'variable': value['variable'],
                            'unit': value['unit'],
                            'type': value['event_type'],
                            'source': value['source'],
                            'name': value['run_name']}
            event_id = get_event_id(db_adapter, db_meta_data)
            #print('event_id:', event_id)
            time_series_df = get_time_series_values(db_adapter, event_id, ts_datetime_from, ts_datetime_to)
            print('df size : ', time_series_df.size)
            error_percentage = (time_series_count - time_series_df.size)/time_series_count
            print('validate_gage_points|error_percentage: ', error_percentage)
            if error_percentage <= MISSING_ERROR_PERCENTAGE:
                new_ts = time_series_df.resample('1H').sum().fillna(0)
                validated_gages[key] = new_ts
            else:
                print('Discard time-series.')
        except Exception as e:
            print("validate_gage_points|Exception|e : ", e)
    return validated_gages


def get_sub_catchment_rain_files():
    db_adapter = MySqlAdapter()
    valid_gages = validate_gage_points(db_adapter, '2018-09-28 00:00:00', '2018-10-01 00:00:00')
    print(valid_gages.keys())
    #get_kub_points_from_meta_data()


def get_sub_ratios():
    try:
        shape_file = 'kub-wgs84/kub-wgs84.shp'
        catchment_file = 'kub/sub_catchments/sub_catchments1.shp'
        kub_points = get_kub_points_from_meta_data()
        thessian_df = get_thessian_polygon_from_gage_points(shape_file, kub_points)
        catchment_df = get_catchment_area(catchment_file)
        sub_ratios = calculate_intersection(thessian_df, catchment_df)
        print(sub_ratios)
        return sub_ratios
    except Exception as e:
        print("get_thessian_polygon_from_gage_points|Exception|e : ", e)


def get_timeseris():
    try:
        shape_file = 'kub-wgs84/kub-wgs84.shp'
        catchment_file = 'kub/sub_catchments/sub_catchments1.shp'
        kub_points = get_kub_points_from_meta_data()
        thessian_df = get_thessian_polygon_from_gage_points(shape_file, kub_points)
        catchment_df = get_catchment_area(catchment_file)
        sub_ratios = calculate_intersection(thessian_df, catchment_df)
        print(sub_ratios)
        db_adapter = MySqlAdapter()
        get_sub_catchment_rainfall('2018-09-27 00:00:00', '2018-09-30 00:00:00', db_adapter, sub_ratios[1])
    except Exception as e:
        print("get_thessian_polygon_from_gage_points|Exception|e : ", e)
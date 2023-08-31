import pydeck as pdk
import pandas as pd
import subprocess, os
from pathlib import Path
from flask import Flask, request, jsonify, make_response, send_file

app = Flask(__name__)

def Phase_1(jsonFile, MinLat, MinLon, MaxLat, MaxLon, MinTime, MaxTime, MinLat_1, MinLon_1, MaxLat_1, MaxLon_1, KNN_ID, KNN_K):
    print(jsonFile, MinLat, MinLon, MaxLat, MaxLon, MinTime, MaxTime, MinLat_1, MinLon_1, MaxLat_1, MaxLon_1, KNN_ID, KNN_K)
    command = "spark-submit phase1/SDSE-Phase-1-assembly-0.1.jar data/output datafile {} get-spatial-range {} {} {} {} get-spatiotemporal-range {} {} {} {} {} {} get-knn {} {}".format(jsonFile, MinLat, MinLon, MaxLat, MaxLon, MinTime, MaxTime, MinLat_1, MinLon_1, MaxLat_1, MaxLon_1, KNN_ID, KNN_K)
    p = subprocess.Popen(command, shell=True)
    p.wait()

def getJsonPath(folder):
    for path in Path(folder).rglob('*.json'):
        return path

def MapLayers(jsonFile,colors):
    layers = []
    with open(jsonFile) as fp:
        for line in fp:
            df = pd.read_json(line)
            minimum_timestamp = df['timestamp'].min().timestamp()

            new_df = pd.DataFrame()

            new_df["coordinates"] = [[[location[1], location[0]] for location in df["location"]]]
            new_df["timestamps"] = [[int(timestamp.timestamp() - minimum_timestamp) for timestamp in df["timestamp"]]]

            layers.append(pdk.Layer(
                "TripsLayer",
                new_df,
                get_path="coordinates",
                get_timestamps="timestamps",
                get_color=colors,
                opacity=0.8,
                width_min_pixels=5,
                rounded=True,
                trail_length=600,
                current_time=500,
            ))

    return layers

@app.route('/', methods = ['GET'])
def index():
    return send_file('main.html')

@app.route('/data', methods = ['GET'])
def getData():
    return send_file(request.args.get(''))

@app.route('/plot', methods = ['POST'])
def plot():
    data = request.get_json(force=True)
    jsonFile = data['jsonFile']
    print("JSONFILEEEEEE", jsonFile)
    MinLat = data['MinLat']
    MinLon = data['MinLon']
    MaxLat = data['MaxLat']
    MaxLon = data['MaxLon']
    MinTime = data['MinTime']
    MaxTime = data['MaxTime']
    MinLat_1 = data['MinLat_1']
    MinLon_1 = data['MinLon_1']
    MaxLat_1 = data['MaxLat_1']
    MaxLon_1 = data['MaxLon_1']
    KNN_ID = data['KNN_ID']
    KNN_K = data['KNN_K']
    print(KNN_ID, KNN_K, "sdssf")
    Phase_1(jsonFile, MinLat, MinLon, MaxLat, MaxLon, MinTime, MaxTime, MinLat_1, MinLon_1, MaxLat_1, MaxLon_1, KNN_ID, KNN_K)
    SpatialRangePath = getJsonPath("data/output/get-spatial-range")
    print(SpatialRangePath)
    SpatioTemporalRangePath = getJsonPath("data/output/get-spatiotemporal-range")
    print(SpatioTemporalRangePath)
    KNNPath = getJsonPath("data/output/get-knn")

    map_layers = []
    map_layers.append(MapLayers(SpatioTemporalRangePath,colors=[255,0,0]))
    map_layers.append(MapLayers(SpatialRangePath,colors=[0,255,0]))
    map_layers.append(MapLayers(KNNPath, colors=[0,0,255]))

    State = pdk.ViewState(latitude = 30, longitude = -100, zoom = 5, bearing = 0, pitch = 45)
    output = pdk.Deck(layers = map_layers, initial_view_state = State)

    render_HTML = output.to_html(as_string = True)
    response = make_response(jsonify({"html": render_HTML}), 201)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

if __name__ == '__main__':
   app.run("localhost", port = 8000, debug = True)
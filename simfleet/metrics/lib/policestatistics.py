import json
from loguru import logger
from simfleet.metrics.basestatistics import BaseStatisticsClass
from simfleet.utils.statistics import Log

class PoliceStatisticsClass(BaseStatisticsClass):
    def run(self, events_log: Log) -> None:
        try:
            self.patrol_metrics(events_log, "simfleet_metrics_patrol.json")
        except Exception as e:
            logger.warning(f"Exception creating metrics: {e}")

    def patrol_metrics(self, events_log: Log, file_path: str):
        filtered_events = events_log.filter(lambda event: event.class_type in
                                                          ["PolicePatrolAgent",
                                                           "EmergencyAgent",
                                                           "FleetManagerAgent"
                                                           ] and
                                            event.event_type in {
                                                "patrol_assigned",
                                                "patrol_moving_to_emergency",
                                                "patrol_arrived",
                                                "patrol_moving",
                                                "emergency_call"
                                            })

        event_fields = ["name", "timestamp", "event_type", "class_type"]
        details_fields = ["distance", "location", "emergency", "patrol"]
        df = filtered_events.to_dataframe(event_fields=event_fields, details_fields=details_fields)

        distances_list = df[df["event_type"] == "patrol_assigned"]["distance"]
        distance_mean = distances_list.mean()

        # Calculate the response mean time (time difference from "patrol_assigned" to "patrol_arrived")
        df_coord = df[df["class_type"] == "FleetManagerAgent"][["emergency", "timestamp"]]
        df_coord = df_coord.rename(columns={"timestamp": "coord_ts"})

        df_patrol = df[df["class_type"] == "PolicePatrolAgent"][["emergency", "timestamp", "name"]]
        df_patrol = df_patrol.rename(columns={"timestamp": "patrol_ts"})

        # Match coord with each patrol for each emergency
        df_merged = df_patrol.merge(df_coord, on="emergency", how="left")

        df_merged["time_diff"] = df_merged["patrol_ts"] - df_merged["coord_ts"]
        response_time_mean = df_merged["time_diff"].mean()

        metrics = {
            "general_metrics": {
                "distance": {
                    "mean": float(distance_mean),
                    "data": distances_list.to_list()
                },
                "response_time": {
                    "mean": float(response_time_mean),
                    "data": df_merged["time_diff"].dropna().to_list()
                }
            }
        }

        df['lat'] = df[df["event_type"] == "emergency_call"]['location'].str[1]
        df['lng'] = df[df["event_type"] == "emergency_call"]['location'].str[0]
        avg_lat = df['lat'].mean()
        avg_lng = df['lng'].mean()

        if int(df['lat'].count()) == 1:
            avg_lng *= 1.0001
            avg_lat *= 1.01

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "name": "Metrics",
                        "data": metrics
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [avg_lat, avg_lng]
                    }
                }
            ]
        }

        routes = (df[df["event_type"] == "patrol_moving"]
                .sort_values(by=["name", "timestamp"])
                .groupby("name")["location"]
                .apply(list)
                .to_dict())

        for key, value in routes.items():
            geojson["features"].append({
                "type": "Feature",
                "properties": {
                    "name": key,
                },
                "geometry": {
                "type": "LineString",
                "coordinates": [v[::-1] for v in value]
              }
            })

        for key, value in df[df["event_type"] == "emergency_call"].set_index('name')['location'].to_dict().items():
            geojson["features"].append({
                "type": "Feature",
                "properties": {
                    "name": key,
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": value[::-1]
                }
            })

        self.export_to_json(geojson, file_path)

    def export_to_json(self, json_data: dict, file_path: str) -> None:
        """
        Export the final JSON structure to a JSON file.

        Args:
            json_data (dict): The data to be exported.
            file_path (str): Path where the JSON file will be saved.
        """
        with open(file_path, 'w') as f:
            json.dump(json_data, f, indent=4)

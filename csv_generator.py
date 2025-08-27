import os
import csv
import json

# Iterate over run directories
for run_dir in filter(lambda run_dir: os.path.isdir(run_dir) and run_dir.startswith('LO2_run'), os.listdir(os.getcwd())):
    # Stores node_exporter metrics (host level metrics)
    NODE_METRICS = dict()
    # Stores cAdvisor metrics (container level metrics)
    CONTAINER_METRICS = dict()
    # Iterate over all tests and their metrics
    for test_name, metrics_dir in filter(lambda x: os.path.isdir(x[1]), ((test_dir, os.path.join(run_dir, test_dir, "metrics")) for test_dir in os.listdir(run_dir))):
        # Iterate over all metric json files
        for metric_json in filter(lambda x: os.path.splitext(x)[1] == ".json" , (os.path.join(metrics_dir, metric_file) for metric_file in os.listdir(metrics_dir))):
            # Load data
            with open(metric_json, 'r') as f:
                try:
                    metric_data = json.load(f)
                except:
                    print(f"Error when loading JSON from {metric_json}")
                    continue
            # Iterate over all metrics stored in json
            for metric in metric_data:
                # Node exporter metrics
                if metric["metric"]["job"] == "node":
                    # Save all fields that describe a metrics as &-separated string of field=value pairs
                    metric_name = [metric["metric"]["__name__"]] + [f"{k}={v}" for k,v in metric["metric"].items() if k not in {"group", "instance", "job", "__name__"}]
                    metric_name = '&'.join(metric_name)
                    for timestamp, value in metric["values"]:
                        # Save the data as  timestamp -> test -> metric -> value
                        test_dict = NODE_METRICS.setdefault(timestamp, dict())
                        metric_dict = test_dict.setdefault(test_name, dict())
                        metric_dict[metric_name] = value
                # cAdvisor metrics
                elif metric["metric"]["job"] == "cadvisor":
                    # Only save metrics that specify the container name
                    if "name" not in metric["metric"]:
                        continue
                    container_name = metric["metric"]["name"]
                    metric_name = metric["metric"]["__name__"]
                    container_dict = CONTAINER_METRICS.setdefault(container_name, dict())
                    for timestamp, value in metric["values"]:
                        # Save the data as  container -> timestamp -> test -> metric -> value
                        test_dict = container_dict.setdefault(timestamp, dict())
                        metric_dict = test_dict.setdefault(test_name, dict())
                        metric_dict[metric_name] = value

    # Get all saved cAdvisor metric names sorted
    all_metrics = set()
    for container_name, container_dict in CONTAINER_METRICS.items():
        for test_dict in container_dict.values():
            for metric_dict in test_dict.values():
                all_metrics = all_metrics.union(metric_dict.keys())
    all_metrics = sorted(list(all_metrics))
    # Prepare the csv header with all metrics
    HEADER = ["timestamp", "run", "container", "test"]
    HEADER.extend(all_metrics)

    # Write metrics for each container
    for container_name, container_dict in CONTAINER_METRICS.items():
        container_output_csv = f"{run_dir}-{container_name}-metrics.csv"
        if os.path.isfile(container_output_csv):
            print(f"{container_output_csv} exists, skipping...")
            continue
        print(f"Writing to {container_output_csv}...")
        # Write csv rows with cAdvisor metrics
        with open(container_output_csv, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(HEADER)
            for timestamp, test_dict in container_dict.items():
                for test_name, metrics_dict in test_dict.items():
                    row = [timestamp, run_dir, container_name, test_name]
                    # Try getting all the metrics that were seen in any test and container
                    for metric in all_metrics:
                        row.append(metrics_dict.get(metric, None))
                csv_writer.writerow(row)

    # Do not overwrite node_exporter metrics
    node_output_csv = f"{run_dir}-node-metrics.csv"
    if os.path.isfile(node_output_csv):
        print(f"{node_output_csv} exists, skipping...")
        continue

    # Get all saved node_exporter metric names sorted
    all_metrics = set()
    for test_dict in NODE_METRICS.values():
        for metric_dict in test_dict.values():
            all_metrics = all_metrics.union(metric_dict.keys())
    all_metrics = sorted(list(all_metrics))
    # Prepare the csv header with all metrics
    HEADER = ["timestamp", "run", "test"]
    HEADER.extend(all_metrics)

    # Write node_exporter metrics
    print(f"Writing to {node_output_csv}...")
    with open(node_output_csv, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(HEADER)
        for timestamp, test_dict in NODE_METRICS.items():
            for test_name, metrics_dict in test_dict.items():
                row = [timestamp, run_dir, test_name]
                # Try getting all the metrics that were seen in any test and container
                for metric in all_metrics:
                    row.append(metrics_dict.get(metric, None))
            csv_writer.writerow(row)

import os
import csv
import json

for run_dir in os.listdir(os.getcwd()):
    if not all([os.path.isdir(run_dir),
                run_dir.startswith('LO2_run')]):
        continue
    NODE_METRICS = dict()
    CONTAINER_METRICS = dict()
    node_output_csv = f"{run_dir}-node-metrics.csv"
    for test_dir in os.listdir(run_dir):
        test_path = os.path.join(run_dir, test_dir)
        if not os.path.isdir(test_path):
            continue
        metrics_dir = os.path.join(test_path, "metrics")
        for metric_file in os.listdir(metrics_dir):
            if os.path.splitext(metric_file)[1] != ".json":
                continue
            metric_json = os.path.join(metrics_dir, metric_file)
            with open(metric_json, 'r') as f:
                try:
                    data = json.load(f)
                except:
                    print(f"Error when loading JSON from {metric_json}")
                    continue
            for metric in data:
                if metric["metric"]["job"] == "node":
                    metric_name = [metric["metric"]["__name__"]] + [f"{k}={v}" for k,v in metric["metric"].items() if k not in {"group", "instance", "job", "__name__"}]
                    metric_name = '&'.join(metric_name)
                    for timestamp, value in metric["values"]:
                        test_dict = NODE_METRICS.setdefault(timestamp, dict())
                        metric_dict = test_dict.setdefault(test_dir, dict())
                        metric_dict[metric_name] = value
                elif metric["metric"]["job"] == "cadvisor":
                    if "name" not in metric["metric"]:
                        continue
                    container_name = metric["metric"]["name"]
                    metric_name = metric["metric"]["__name__"]
                    container_dict = CONTAINER_METRICS.setdefault(container_name, dict())
                    for timestamp, value in metric["values"]:
                        test_dict = container_dict.setdefault(timestamp, dict())
                        metric_dict = test_dict.setdefault(test_dir, dict())
                        metric_dict[metric_name] = value

    if os.path.isfile(node_output_csv):
        print(f"{node_output_csv} exists, skipping...")
    else:
        all_metrics = set()
        for test_dict in NODE_METRICS.values():
            for metric_dict in test_dict.values():
                all_metrics = all_metrics.union(metric_dict.keys())
        all_metrics = sorted(list(all_metrics))
        HEADER = ["timestamp", "run", "test"]
        HEADER.extend(all_metrics)

        print(f"Writing to {node_output_csv}...")
        with open(node_output_csv, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(HEADER)
            for timestamp, test_dict in NODE_METRICS.items():
                for test_name, metrics_dict in test_dict.items():
                    row = [timestamp, run_dir, test_name]
                    for metric in all_metrics:
                        row.append(metrics_dict.get(metric, None))
                csv_writer.writerow(row)

    for container_name, container_dict in CONTAINER_METRICS.items():
        container_output_csv = f"{run_dir}-{container_name}-metrics.csv"
        all_metrics = set()
        for test_dict in container_dict.values():
            for metric_dict in test_dict.values():
                all_metrics = all_metrics.union(metric_dict.keys())
        all_metrics = sorted(list(all_metrics))
        HEADER = ["timestamp", "run", "container", "test"]
        HEADER.extend(all_metrics)

        print(f"Writing to {container_output_csv}...")
        with open(container_output_csv, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(HEADER)
            for timestamp, test_dict in container_dict.items():
                for test_name, metrics_dict in test_dict.items():
                    row = [timestamp, run_dir, container_name, test_name]
                    for metric in all_metrics:
                        row.append(metrics_dict.get(metric, None))
                csv_writer.writerow(row)


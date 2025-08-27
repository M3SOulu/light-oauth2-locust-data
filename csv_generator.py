import os
import csv
import json

for run_dir in os.listdir(os.getcwd()):
    if not all([os.path.isdir(run_dir),
                run_dir.startswith('LO2_run')]):
        continue
    for test_dir in os.listdir(run_dir):
        test_path = os.path.join(run_dir, test_dir)
        output_csv = f"{run_dir}-{test_dir}.csv"
        if os.path.isfile(output_csv):
            print(f"{output_csv} exists, skipping...")
            continue
        if not os.path.isdir(test_path):
            continue
        METRICS = dict()
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
                if metric["metric"]["job"] != "node":
                    continue
                metric_name = metric["metric"]["__name__"] + "&" + "&".join([f"{k}={v}" for k,v in metric["metric"].items() if k not in {"group", "instance", "job", "__name__"}])
                metric_name = metric_name.removesuffix("&")
                for timestamp, value in metric["values"]:
                    metric_values = METRICS.setdefault(timestamp, dict())
                    metric_values[metric_name] = value

        all_metrics = set()
        for timestamp, metrics_dir in METRICS.items():
            all_metrics = all_metrics.union(metrics_dir.keys())
        all_metrics = sorted(list(all_metrics))
        HEADER = ["timestamp", "run_start", "test_name"]
        HEADER.extend(all_metrics)

        print(f"Writing to {output_csv}...")
        with open(output_csv, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(HEADER)
            for timestamp, metrics_dir in METRICS.items():
                row = [timestamp, run_dir, test_dir]
                for metric in all_metrics:
                    row.append(metrics_dir.get(metric, None))
                csv_writer.writerow(row)

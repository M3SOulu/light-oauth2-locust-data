import os
import pandas as pd

all_dfs = []
for run_name in os.listdir(os.getcwd()):
    if not os.path.isdir(run_name) or not run_name.startswith("light-oauth2"):
        continue
    run_dfs = []
    for test_name in os.listdir(run_name):
        if not os.path.isdir(os.path.join(run_name,test_name)):
            continue
        run_dfs.append(pd.read_csv(f"{run_name}-{test_name}.csv", index_col=False))
        print(f"Added {run_name}-{test_name}.csv")
    result = pd.concat(run_dfs, axis=0, ignore_index=True)
    result = result.sort_values(by=['timestamp'])
    print(f"Saving {run_name}.csv")
    result.to_csv(f"{run_name}.csv", header=True, index=False)
    all_dfs.append(result)

print("Processing entire data")
result = pd.concat(all_dfs, axis=0, ignore_index=True)
result = result.sort_values(by=['timestamp'])
print("Saving light-oauth2-data.csv")
result.to_csv("light-oauth2-data.csv", header=True, index=False)

import pandas as pd

from sklearn.decomposition import PCA
import matplotlib.pyplot as plt


def top_features(component_df, components, top):
    selected_features = set()
    for i in range(components):
        print(f"Top {top} features contributing to PC{i}:")
        values = component_df.iloc[i].abs().sort_values(ascending=False).head(top)
        print(values)
        selected_features.update(values.index)
    selected_features = sorted(list(selected_features))
    if components > 1:
        print(f"Selected {len(selected_features)} features from TOP-{top} of {components} best components:")
        for i in selected_features:
            print(i)
    return selected_features


def calc_pca(df):
    pca = PCA()

    features = df.values
    result = pca.fit_transform(features)
    components = pca.components_
    variance_ratio = pca.explained_variance_ratio_
    cumulative_variance = variance_ratio.cumsum()
    component_df = pd.DataFrame(components, columns=df.columns)
    plt.plot(cumulative_variance[:15])
    return result, component_df, variance_ratio, cumulative_variance


def main(df, col_to_drop, TOP_features, N_components):
    # Raw data
    df = df.drop(columns=col_to_drop)
    print("Rows:", len(df))
    print("Cols:", len(df.columns))

    _, component_df, _, _ = calc_pca(df)

    selected_features = top_features(component_df, components=N_components, top=TOP_features)

    plt.show()


if __name__ == "__main__":
    df = pd.read_csv("LO2_run_1739743201-node-metrics.csv", index_col=None)
    TOP_features = 5
    N_components = 3  # From the plot 3 components are enough
    col_to_drop = ['timestamp', 'run', 'test']
    print("Host metrics")
    main(df, col_to_drop, TOP_features, N_components)
    col_to_drop = ['timestamp', 'run', 'container', 'test']
    TOP_features = 10
    N_components = 1  # From the plot 1 component is enough
    for container in ["mysqldb",
                      "oauth2-client",
                      "oauth2-code",
                      "oauth2-key",
                      "oauth2-refresh-token",
                      "oauth2-service",
                      "oauth2-token",
                      "oauth2-user"]:
        print(f"\n{container}")
        df = pd.read_csv(f"LO2_run_1739743201-light-oauth2-{container}-1-metrics.csv", index_col=None)
        main(df, col_to_drop, TOP_features, N_components)


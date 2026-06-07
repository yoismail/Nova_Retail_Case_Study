from functools import reduce


def scale_data(df, multiplier):
    """Scale a DataFrame by creating multiple copies and unioning them together."""
    dfs = [df] * multiplier

    return reduce(
        lambda x, y: x.union(y),
        dfs
    )

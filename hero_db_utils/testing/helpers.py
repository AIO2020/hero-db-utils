import pandas.testing as pd_testing


def assert_df_equal(
    a,
    b,
    ignore_idx=False,
    ignore_dtypes=False,
    ignore_cols_order=False,
    **pd_test_kwargs
):
    """
    Checks that two dataframes contain the same data.

    Parameters
    ----------
    `a`: pd.DataFrame
        First dataframe to compare.
    `b`: pd.DataFrame
        Second dataframe to compare.
    `ignore_idx`: bool, default False
        If True the index of both dataframes will be dropped before the comparison.
    `ignore_dtypes`: bool, default False
        Ignore differing dtypes in columns.
    `ignore_cols_order`: bool, default False
        If True the columns will be sorted before the comparison.
    `**pd_test_kwargs`: Any.
        Named arguments to pass to the `pandas.testing.assert_frame_equal function`.
    
    Raises
    ------
    `AssertionError`

    """
    a = a.copy()
    b = b.copy()
    if ignore_idx:
        a.reset_index(drop=True, inplace=True)
        b.reset_index(drop=True, inplace=True)
    if ignore_dtypes:
        # Convert all columns to strings:
        pd_test_kwargs["check_dtype"] = False
    if ignore_cols_order:
        a.sort_index(axis=1, inplace=True)
        b.sort_index(axis=1, inplace=True)
    return pd_testing.assert_frame_equal(a, b, **pd_test_kwargs)

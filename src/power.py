#
# This code was generated with assistance from [Claude 3.5 Sonnet] (Anthropic)
# Generation Date: Oct 29, 2024
# Modifications: Consolidated functions
#

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.power import TTestPower
from statsmodels.tsa.ar_model import AutoReg, AutoRegResultsWrapper


def power_reg(model, delta, exog=None, **kwargs):
    """
    Perform power analysis on fitted model.
    
    Parameters:
    -----------
    model : RegResults
        Fitted model.
    delta : float
        The change to the sum. Not the effect size (change to mean).
    exog : str
        Variable to test power coef.
    alpha : float
        Significance level (default: 0.05)
    power : float
        Desired statistical power (default: 0.8)
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing power analysis stats_df
    """
    if isinstance(model, AutoRegResultsWrapper):
        ar_names = model.model.exog_names
        is_lag = lambda n: model.model.endog_names + ".L" in n
        ar_params = [p for p,n in zip(model.params, ar_names) if is_lag(n)]
        n = len(model.resid)
        n = n * (1 - sum(ar_params)) / (1 + sum(ar_params))
        residual_var = model.sigma2
    else:
        n = model.nobs
        residual_var = model.mse_resid
    if exog is not None:
        exog_idx = model.model.exog_names.index(exog)
        model_stats = pd.Series({"mean": 0, "std": model.bse.iloc[exog_idx]})
    else:
        model_stats = pd.Series({"mean": model.data.endog.mean(), 
                             "std": np.sqrt(residual_var)})
    power_stats = power_analysis(model_stats['mean'],
                                model_stats['std'],
                                n, delta, **kwargs)
    return pd.concat([model_stats, power_stats])


def power_uncond(data, group_cols, value_col, delta, **kwargs):
    """
    Perform power analysis on grouped data (unconditional gaussian).
    
    Parameters:
    -----------
    data : pandas.DataFrame
        Input dataframe containing the data
    group_cols : list
        List of columns to group by
    value_col : str
        Name of the column containing the values to analyze
    delta : float
        The change to the sum. Not the effect size (change to mean).
    alpha : float
        Significance level (default: 0.05)
    power : float
        Desired statistical power (default: 0.8)
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing power analysis stats_df
    """
    stats_df = data.groupby(group_cols)[value_col].agg([
        'mean',
        'std',
        'sum',
        'count'
    ])
    power_stats = power_analysis(stats_df['mean'],
                          stats_df['std'],
                          stats_df['count'],
                          delta, **kwargs)
    return pd.concat([stats_df.reset_index(), power_stats], axis=1)
    

def power_analysis(mean, std, n, delta, alpha=0.05, power=0.8):
    """
    Perform power analysis.
    
    Parameters:
    -----------
    delta : float
        The change to the sum. Not the effect size (change to mean).
    alpha : float
        Significance level (default: 0.05)
    power : float
        Desired statistical power (default: 0.8)
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing power analysis stats_df
    """
    # Calculate standard error
    std_err = std / np.sqrt(n)
    
    # Critical values
    z_alpha = stats.norm.ppf(1 - alpha/2)  # Two-tailed test
    z_beta = stats.norm.ppf(power)
    
    # Calculate MDE
    mde = (z_alpha + z_beta) * std_err
    
    # Calculate new means with effect
    effect_mean = ((mean * n) + delta) / n
    effect_size = effect_mean - mean
    
    # Calculate test statistics
    t_stat = effect_size / std_err
    
    # Calculate achieved power
    z_alpha = stats.norm.ppf(1 - alpha/2)
    ncp = effect_size / std_err
    power_achieved = stats.norm.cdf(ncp - z_alpha)
    
    # Calculate p-value (two-tailed test)
    p_value = 2 * (1 - stats.norm.cdf(abs(t_stat)))
    
    # Determine if effect is detectable
    is_detectable = (
        (power_achieved >= power) & 
        (effect_size >= mde) &
        (p_value < alpha)

    )

    cols = ['mde', 'effect_size', 'power_achieved', 't_stat', 'is_detectable']
    result = (mde, effect_size, power_achieved, t_stat, is_detectable)
    if isinstance(mean, pd.Series):
        return pd.DataFrame(np.vstack(result).T, columns=cols)
    else:
        return pd.Series(result, index=cols)
import re
import pandas as pd
from statsmodels.graphics.dotplots import dot_plot
import matplotlib.pyplot as plt

def catvar(df, name, ref=None):
    """Helper function to produce patsy formula for categorical variable."""
    if ref is None:
        return f"C({name})"
    else:
        ref = pd.Categorical(df[name]).categories.get_loc(ref)
        return f"C({name}, Treatment(reference={ref}))"

def prettify_coefs(coefs: list[str]) -> list[str]:
    """Strip away annoying patsy junk."""
    if isinstance(coefs, list):
        return list(map(_prettify_coef, coefs))
    return coefs.map(_prettify_coef)


def _prettify_coef(name: str) -> str:
    var_pat = r"[a-zA-Z_\.]+"
    cat_ref_pat = r"C\(({}), Treatment\(reference=[0-9]+\)\)\[T\.({})\]".format(var_pat, var_pat)
    cleaned_name = re.sub(cat_ref_pat, r"\1.\2", name)
    cat_pat = r"({})\[T\.({})\]".format(var_pat, var_pat)
    cleaned_name = re.sub(cat_pat, r"\1.\2", cleaned_name)
    return cleaned_name

def _factorname(factor, level):
    if level == True:
        return factor
    elif level == "Intercept":
        return "Intercept"
    else:
        return factor + '.' + level

def coefplot(model):
    """Create coefficient plot from fitted model."""
    model_stats = pd.concat([model.conf_int().rename(columns={0:'left',1:'right'}), model.params.rename('mean')], axis=1)
    model_stats['left'] = abs(model_stats['left'] - model_stats['mean'])
    model_stats['right'] = abs(model_stats['right'] - model_stats['mean'])
    fmt_func = lambda i: _prettify_coef(model.model.exog_names.__getitem__(i))
    fig = dot_plot(model.params, 
                   model_stats[['left','right']], 
                   fmt_left_name=fmt_func)
    plt.axvline(0, linestyle='dashed')
    return fig

def main_effects(model, factor, ref_level):
    names = prettify_coefs(model.model.exog_names)
    is_level = lambda x: x.count('.') == (1 if isinstance(ref_level, str) else 0)
    is_factor = lambda x: x.startswith(factor) and is_level(x)
    mask = list(map(is_factor, names))
    level_names = list(filter(is_factor, names))
    level_names = [x.removeprefix(factor + '.') for x in level_names]
    ref_coef = model.params['Intercept']
    level_coefs = model.params[mask] + ref_coef
    effects = dict(zip(level_names, level_coefs))
    effects |= {ref_level: ref_coef}
    return pd.Series(effects)


def cond_effect(model, factor_a, ref_a, factor_b, level_b):
    raise NotImplementedError()
    main = main_effects(model, factor_a, ref_a)
    joint = {lvl: joint_effect(model, factor_a, lvl, factor_b, level_b) for lvl in main.index}
    return pd.Series(joint)

def joint_effect(model, factor_a, level_a, factor_b, level_b):
    names = prettify_coefs(model.model.exog_names)
    # XXX: hacky way to deal with bools. doesn't support joint effect of False yet!
    name_a = _factorname(factor_a, level_a)
    name_b = _factorname(factor_b, level_b)
    level_a_idx = names.index(name_a)
    level_b_idx = names.index(name_b)
    # XXX: not robust to opposite vaiable order.
    level_ab_idx = names.index(name_a + ":" + name_b)
    return model.params.iloc[[0, level_a_idx, level_b_idx, level_ab_idx]].sum()
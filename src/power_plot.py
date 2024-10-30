# Plotly code to create histograms comparing simulated data under H0 and H1,
# and to highlight the H0 mean, H1 mean, and minimum detectable effect size.
# We don't have anything close to enough power yet so no need to show.

import numpy as np
import pandas as pd
import plotly.express as px
from typing import Union

def plot_power(power: pd.DataFrame, labels: Union[list, pd.Series]):
    """
    Params:
        - power: requires columns {mean, effect_size, std, mde}
        - labels: what do we call the mu's and sigma's
    """
    samples_h0 = _sample_normal(power['mean'], power['std'], 1000)
    samples_h1 = _sample_normal(power['mean']+power['effect_size'], power['std'], 1000)
    samples = pd.concat([pd.DataFrame(samples_h0.T, columns = labels),
                        pd.DataFrame(samples_h1.T, columns = labels)],
                        keys=['H0','H1'], names=['hyp'])
    samples.index = samples.index.set_names(['hyp','sample'])
    samples = samples.reset_index().melt(id_vars=['hyp','sample'])

    fig = px.histogram(samples, nbins=50, x='value', facet_row='variable', color='hyp')
    for i,x in enumerate(power.itertuples()):
        fig.add_vline(x=x.mean, row=(len(power)-i), line_dash="dot", line_color="black")
        fig.add_vline(x=x.mean + x.mde, row=(len(power)-i), line_dash="dot", line_color="gray")
    fig = fig.update_yaxes(matches=None, title='', showticklabels=False)
    for facet_title in fig.layout['annotations']:
        facet_title['textangle'] = 0
        facet_title['text'] = facet_title['text'].removeprefix('variable=')
    fig.show()

def _sample_normal(mu: pd.Series, sigma: pd.Series, n: int):
    return np.random.normal(mu.values[:, np.newaxis], sigma.values[:, np.newaxis], (len(mu), 1000))
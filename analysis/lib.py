import matplotlib.patches
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import textwrap


def wrap_labels(ax, width, break_long_words=False):
    labels = []
    for label in ax.get_xticklabels():
        text = label.get_text()
        labels.append(textwrap.fill(text, width=width,
                      break_long_words=break_long_words))
    ax.set_xticklabels(labels, rotation=0)

def wrap_axes_labels(ax, width, break_long_words=False):
    x, y = ax.get_xlabel(), ax.get_ylabel()
    if x is not None:
        ax.set_xlabel(
            textwrap.fill(x, width=width, break_long_words=break_long_words)
        )

    if y is not None:
        ax.set_ylabel(
            textwrap.fill(y, width=width, break_long_words=break_long_words)
        )


def adjust_pairplot_axes(df, columns, plot):
    lims = []
    for m in columns:
        m_min, m_max = df.loc[:, m].min(), df.loc[:, m].max()
        margin = 0.025 * (m_max - m_min)
        lims.append([m_min - margin, m_max + margin])

    for row in range(len(columns)):
        for col in range(len(columns)):
            ax = plot.axes[row, col]
            if ax is None:
                continue
            wrap_axes_labels(ax, 20)
            if row == col:
                continue
            ax.set_xlim(lims[col])
            ax.set_ylim(lims[row])

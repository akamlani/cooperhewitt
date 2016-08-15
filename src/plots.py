import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("darkgrid")
sns.set_context("talk")
sns.set_palette("husl")

import plotly.plotly as py
import plotly.graph_objs as go


class Display(object):
    def __init__(self):
        pass

    def create_subplots(self, params, (nrows, ncols, plt_size), filename='plots/pen_eda.png'):
        fig, _ = plt.subplots(nrows, ncols, figsize=plt_size)

        for idx, (ax, param_i) in enumerate(zip(fig.axes, params)):
            if param_i.get('type') == 'bar':
                if (param_i.get('transform')) == 'Log':
                    sns.barplot(param_i['frame'].index, np.log(param_i['frame']), ax=ax )
                    ax.set_yscale('log')
                else:
                    sns.barplot(param_i['frame'].index, param_i['frame'], ax=ax )
            elif param_i.get('type') == 'hbar':
                sns.barplot(param_i['frame'],  list(param_i['frame'].index), ax=ax , orient='h')
            else:
                ax.plot(param_i['frame'].index, param_i['frame'])

            y_label = param_i['ylabel'] + " (" +  param_i.get('transform') + ")" if param_i.get('transform') else param_i['ylabel']
            ax.set_ylabel(y_label)
            ax.set_xlabel(param_i['xlabel'])
            if param_i.get('title'): ax.set_title(param_i.get('title'))
            if param_i.get('rot'):
                for item in ax.get_xticklabels(): item.set_rotation(param_i.get('rot'))
            if param_i.get('limits'):
                tup = param_i.get('limits')
                ax.set_xlim(tup[0], tup[1])
            if param_i.get('labels'):
                items = param_i.get('labels')
            ax.tick_params(axis='y', which='major', pad=15)

        #fig.delaxes(fig.axes[nrows*ncols-1])
        plt.subplots_adjust(bottom=0.05)
        plt.tight_layout()
        fig.savefig(filename, dpi=100)

    def plotly_graph(self, gx, filename):
        axis=dict(showbackground=False, showline=False, zeroline=False, showgrid=False, showticklabels=False, title='')
        trace1=go.Scatter3d(x=gx.Xe,y=gx.Ye,z=gx.Ze, mode='lines',
                            line=go.Line(color='rgb(125,125,125)', width=1), hoverinfo='none')
        trace2=go.Scatter3d(x=gx.Xn,y=gx.Yn,z=gx.Zn, mode='markers',name='artwork',
                         marker=go.Marker(symbol='diamond',size=6,color=gx.group,colorscale='Viridis',
                                       line=go.Line(color='rgb(50,50,50)', width=0.5)),
                         text=gx.labels, hoverinfo='text',)

        layout_grid = go.Layout(title="",
                                width=1000, height=1000, showlegend=False,
                                scene=go.Scene( xaxis=go.XAxis(axis),yaxis=go.YAxis(axis),zaxis=go.ZAxis(axis)),
                                margin=go.Margin(t=35,b=0), hovermode='closest',
                                paper_bgcolor='rgb(233,233,233)',
                                annotations=go.Annotations([
                                    go.Annotation(
                                       showarrow=False,
                                       text="",
                                       xref='paper',yref='paper',x=0,y=0.01,xanchor='left',yanchor='bottom',
                                       font=go.Font(size=14) )
                                ]), )

        data=go.Data([trace1, trace2])
        fig=go.Figure(data=data, layout=layout_grid)
        py.iplot(fig, filename=filename)
        py.image.save_as(fig, filename=filename)
        return fig







# create subplots based on passed in data configuration
# sns.palplot(sns.color_palette())
# https://stanford.edu/~mwaskom/software/seaborn/tutorial/color_palettes.html#palette-tutorial

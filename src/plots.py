import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("darkgrid")
sns.set_context("talk")
sns.set_palette("husl")

class Display(object):
    def __init__(self):
        pass

    def create_subplots(self, params, (nrows, ncols, plt_size) ):
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
        fig.savefig('../plots/pen_eda.png', dpi=100)





# create subplots based on passed in data configuration
# sns.palplot(sns.color_palette())
# https://stanford.edu/~mwaskom/software/seaborn/tutorial/color_palettes.html#palette-tutorial

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
        
        for ax, param_i in zip(fig.axes, params):
            if param_i.get('type') == 'bar':
                if (param_i.get('transform')) == 'Log':
                    sns.barplot(param_i['frame'].index, np.log(param_i['frame']), ax=ax )
                    ax.set_yscale('log')
                else:
                    sns.barplot(param_i['frame'].index, param_i['frame'], ax=ax )
            else:
                ax.plot(param_i['frame'].index, param_i['frame'])
            
            y_label = param_i.get('transform') + " " + param_i['ylabel'] if param_i.get('transform') else param_i['ylabel']
            ax.set_ylabel(y_label)
            ax.set_xlabel(param_i['xlabel'])
            if param_i.get('title'): ax.set_title(param_i.get('title'))
            if param_i.get('rot'):
                for item in ax.get_xticklabels(): item.set_rotation(param_i.get('rot'))
        plt.tight_layout()





# create subplots based on passed in data configuration
# sns.palplot(sns.color_palette())
# https://stanford.edu/~mwaskom/software/seaborn/tutorial/color_palettes.html#palette-tutorial

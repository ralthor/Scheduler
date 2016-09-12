import numpy as np

import matplotlib.pyplot as pyplot
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle

def max_min(bars):
    min_b = bars[0][0]
    max_b = min_b
    for bar in bars:
        for x in bar:
            if x < min_b:
                min_b = x
            elif x > max_b:
                max_b = x
    return min_b, max_b

def plot(x_list, y_list, xlable, ylable, legend=None, legend_place='best',
         pdf_name=None, show=True, figure_num=None, show_title=False,
         start_y_from_zero=False, do_low_high=True, new_low=None, new_high=None):
    figure = pyplot.figure(num=figure_num, figsize=(6, 3.5), dpi=80)
    ax1 = figure.add_subplot(111)
    pyplot.xlabel(xlable)
    pyplot.ylabel(ylable)
    pyplot.tight_layout()
    pyplot.grid(True,axis='both')
    line_spec = ['b.-', 'rx-', 'go-', 'md-', 'b.-', 'rx-', 'go-', 'md-']

    # ax1.yaxis
    ax1.ticklabel_format(axis='y', style='sci', useOffset=False)

    if do_low_high:
        low, high = max_min(y_list)
        new_low = (low-0.2*(high-low))
        new_high = (high+0.2*(high-low))
        if new_low < 0:
            low = 0

    if new_low is not None:
        pyplot.ylim([new_low, new_high])

    for i, x in enumerate(x_list):
        y = y_list[i]
        pyplot.plot(x, y, line_spec[i])

    if legend is not None:
        pyplot.legend(legend, legend_place)
    if pdf_name is not None:
        if show_title:
            pyplot.title(pdf_name)
        pdf = PdfPages(pdf_name + '.pdf')
        pdf.savefig()
        pdf.close()

    if show:
        pyplot.show()
# =========== SAMPLE: ============ #

# x1 = np.arange(1, 10.1, 0.1)
# y1 = np.sin(x1)
#
# x2 = np.arange(1, 10.1, 0.1)
# y2 = np.tan(x2)
# plot([x1, x2], [y1, y2], 'Utilization', 'Slowdown', ('Sin', 'Tan'), 'upper left')
#

def bar(bars, title, xlabel, ylabel, xticklabels, legend,
        legend_place='best', width=0.15, pdf_name=None, do_low_high=True):
    n = len(bars[0])

    ind = np.arange(n)  # the x locations for the groups
    # width = 0.15       # the width of the bars

    fig, ax = pyplot.subplots()

    pyplot.tight_layout()
    pyplot.grid(True,axis='both')

    ax.ticklabel_format(axis='y', style='sci', useOffset=False)

    if do_low_high:
        low, high = max_min(bars)
        new_low = (low-0.2*(high-low))
        new_high = (high+0.2*(high-low))
        if new_low < 0:
            low = 0

        pyplot.ylim([new_low, new_high])

    colors = ['r', 'y', 'm', 'b']

    for i, single_bar in enumerate(bars):
        ax.bar(ind + i * width, single_bar, width, color=colors[i])

    # add some text for labels, title and axes ticks
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    ax.set_xticks(ind + len(bars) / 2.0 * width)
    ax.set_xticklabels(xticklabels)

    ax.legend(legend, legend_place)

    pyplot.tight_layout()
    pyplot.legend(legend, legend_place)

    if pdf_name is not None:
        pdf = PdfPages(pdf_name)
        pdf.savefig()
        pdf.close()

    pyplot.show()


# =========== SAMPLE: ============ #

# bars = [(20, 35, 30, 35, 27), (25, 32, 34, 20, 25), (23, 27, 24, 30, 15), (12, 45, 17, 9, 16)]
# xticklabels = ('G1', 'G2', 'G3', 'G4', 'G5')
# xlabel = 'groups'
# ylabel = 'Scores'
# title = 'some fancy title'
# legend = ('Men', 'Women', 'Other', 'Another')
#
# bar(bars, title, xlabel, ylabel, xticklabels, legend)

def add_rect(resource, start_time, finish_time, color, tickness=0.1):
    pyplot.gca().add_patch(Rectangle(
            (start_time, resource * tickness), # lower left point of rectangle
            finish_time - start_time, tickness,   # width/height of rectangle
            # transform=ax.transAxes,
            facecolor=color,
            edgecolor=color,
            alpha=0.75,
            zorder=2,))

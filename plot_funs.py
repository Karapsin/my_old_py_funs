import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def add_value_labels(ax, graph_type, add_str='', box=True):

    if graph_type=='bar':
        # For each bar: Place a label
        for rect in ax.patches:
            # Get X and Y placement of label from rect.
            y_value = rect.get_height()
            x_value = rect.get_x() + rect.get_width() / 2

            # Number of points between bar and label. Change to your liking.
            space = 5
            # Vertical alignment for positive values
            va = 'bottom'

            # If value of bar is negative: Place label below bar
            if y_value < 0:
                # Invert space to place label below
                space *= -1
                # Vertically align label at top
                va = 'top'

            # Use Y value as label and format number with one decimal place
            label = "{:,}".format(y_value)+add_str

            # Create annotation
            ax.annotate(
                label,                      # Use `label` as label
                (x_value, y_value),         # Place label at end of the bar
                xytext=(0, space),          # Vertically shift label by `space`
                textcoords="offset points", # Interpret `xytext` as offset in points
                ha='center',                # Horizontally center label
                va=va,
                Bbox = (dict(facecolor = 'white', alpha=1) if box else None),
                fontsize=8
            ) 
            
    elif graph_type=='line':
        x=ax.lines[0].get_xdata().tolist()
        y=ax.lines[0].get_ydata().tolist()
        
        for i in range(len(x)):
            ax.annotate("{:,}".format(y[i])+add_str,
                        xy=(x[i], y[i]+1),
                        textcoords='data',
                        Bbox = (dict(facecolor = 'white', alpha=1) if box else None),
                        fontsize=8
            )


def get_bar_and_line_chart (df, 
                            cols_to_bars,
                            bars_colors,
                            col_to_line,
                            line_color,
                            x_labels_col,
                            line_to_second_axis=True,
                            min_x=None,
                            max_x=None,
                            min_y=None,
                            max_y=None,
                            min_y_right=None,
                            max_y_right=None,
                            width = 0.65,
                            show_legend=True,
                            plot_title=None,
                            label_bars=False,
                            label_bars_add_str='',
                            label_bars_box=True,
                            label_line=False,
                            label_line_add_str='',
                            label_line_box=False,
                            left_y_axis_text=True,
                            right_y_axis_text=True,
                            img_resolution=None,
                            grid=False,
                            line_width=None,
                            dpi=100
     ):
    
    if min_x is None:
        min_x=(-0.5)*(len(cols_to_bars)-1)
     
    if max_x is None:
        max_x=(df.shape[0])
        
    if line_to_second_axis:
        
        if min_y_right is None:
            min_y_right=min(df[col_to_line].tolist())-min(df[col_to_line].tolist())*3 
            
        if max_y_right is None:
            max_y_right=max(df[col_to_line].tolist())+max(df[col_to_line].tolist())*0.32
        
    if min_y is None:        
        calc=min(df[cols_to_bars].max(axis=1).tolist())-min(df[cols_to_bars].max(axis=1).tolist())*0.5
        min_y=calc if min(df[cols_to_bars].max(axis=1).tolist())<0 else 0
    
    if max_y is None:
        max_y=max(df[cols_to_bars].max(axis=1).tolist())+max(df[cols_to_bars].max(axis=1).tolist())*0.5
       
    ax1=df[cols_to_bars].plot(kind='bar', width = width, color=bars_colors)
    ax2=df[col_to_line].plot(secondary_y=line_to_second_axis, color=line_color, marker='o')

    ax1.set_xticklabels(df[x_labels_col])

    if line_width is not None:
        for line in plt.gca().lines:
            line.set_linewidth(line_width)
    
    #подписи 
    if label_bars:
        add_value_labels(ax1, 'bar', label_bars_add_str, label_bars_box)
        
    if label_line:
        add_value_labels(ax2, 'line', label_line_add_str, label_line_box)

    #убираем текст и метки с левой оси
    if not(left_y_axis_text):
        ax1.set_yticklabels([])
        ax1.tick_params(left=False)

    #убираем текст и метки с правой оси
    if not(right_y_axis_text):
        plt.yticks(color="white")
        plt.tick_params(right=False)

    #mix, max y 
    ax1.set_ylim(min_y, max_y)
    if line_to_second_axis:
        ax2.set_ylim(min_y_right, max_y_right)

    #min, max x
    ax1.set_xlim(min_x, max_x)

    if not(show_legend):
        ax1.get_legend().remove()

    if plot_title is not None:
        plt.title(plot_title, loc='left')
   
    #настройка разрешения    
    if img_resolution is None:
        res=plt.gcf()
        res.set_dpi(dpi)
    else:
        res=plt.gcf()
        res.set_dpi(dpi)
        res.set_size_inches(img_resolution[0]/dpi, img_resolution[1]/dpi)
        
    if grid:
        plt.grid()
        
    return plt.gcf()

def get_line_chart (df, 
                    cols_to_lines,
                    lines_colors,
                    x_labels_col,
                    cols_to_lines_second_axis=None,
                    lines_colors_second_axis=None,
                    min_x=None,
                    max_x=None,
                    min_y=None,
                    max_y=None,
                    min_y_right=None,
                    max_y_right=None,
                    show_legend=True,
                    plot_title=None,
                    label_lines=False,
                    label_lines_second_axis=False,
                    left_y_axis_text=True,
                    right_y_axis_text=True,
                    dpi=100,
                    img_resolution=None,
                    grid=False,
                    lines_width=None
     ):
    
    second_axis_exists=True if (not(cols_to_lines_second_axis is None) and not(lines_colors_second_axis is None)) else False
    
    if min_x is None:
        min_x=(-0.5)*(len(cols_to_lines)-1)
     
    if max_x is None:
        max_x=(df.shape[0])
        
    if second_axis_exists:
        
        if min_y_right is None:
            min_y_right=min(df[cols_to_lines_second_axis].tolist())-min(df[cols_to_lines_second_axis].tolist())*3 
            
        if max_y_right is None:
            max_y_right=max(df[cols_to_lines_second_axis].tolist())+max(df[cols_to_lines_second_axis].tolist())*0.32
        
    if min_y is None:        
        calc=min(df[cols_to_lines].max(axis=1).tolist())-min(df[cols_to_lines].max(axis=1).tolist())*0.5
        min_y=calc if min(df[cols_to_lines].max(axis=1).tolist())<0 else 0
    
    if max_y is None:
        max_y=max(df[cols_to_lines].max(axis=1).tolist())+max(df[cols_to_lines].max(axis=1).tolist())*0.5
       
    ax1=df[cols_to_lines+[x_labels_col]].plot(secondary_y=False, color=lines_colors, marker='o', x=x_labels_col)
    x_axis=ax1.xaxis
    x_axis.label.set_visible(False)
    
    if lines_width is not None:
        for line in plt.gca().lines:
            line.set_linewidth(lines_width)

    
    if second_axis_exists:
        ax2=df[cols_to_lines].plot(secondary_y=True, color=lines_colors, marker='o')

    #подписи 
    if label_lines:
        add_value_labels(ax1, 'line')
        
    if second_axis_exists and label_lines_second_axis:   
        add_value_labels(ax2, 'line')

    #убираем текст и метки с левой оси
    if not(left_y_axis_text):
        ax1.set_yticklabels([])
        ax1.tick_params(left=False)

    #убираем текст и метки с правой оси
    if second_axis_exists and not(right_y_axis_text):
        plt.yticks(color="white")
        plt.tick_params(right=False)

    #mix, max y 
    ax1.set_ylim(min_y, max_y)
    if second_axis_exists:
        ax2.set_ylim(min_y_right, max_y_right)

    #min, max x
    ax1.set_xlim(min_x, max_x)

    if not(show_legend):
        ax1.get_legend().remove()
        
    if plot_title is not None:
        plt.title(plot_title, loc='left')
        
    #настройка разрешения    
    if img_resolution is None:
        res=plt.gcf()
        res.set_dpi(dpi)
    else:
        res=plt.gcf()
        res.set_dpi(dpi)
        res.set_size_inches(img_resolution[0]/dpi, img_resolution[1]/dpi)
        
    if grid:
        plt.grid()
    
    return res

def get_bar_chart(df, 
                  cols_to_bars,
                  bars_colors,
                  x_labels_col,
                  min_x=None,
                  max_x=None,
                  min_y=None,
                  max_y=None,
                  min_y_right=None,
                  max_y_right=None,
                  label_bars=False,
                  label_bars_add_str='',
                  label_bars_box=True,
                  width = 0.65,
                  show_legend=True,
                  plot_title=None,
                  y_axis_text=True,
                  dpi=100,
                  img_resolution=None,
                  grid=False,
                  rotation=0
     ):
    
    if min_x is None:
        min_x=(-0.5)*(len(cols_to_bars)-1)
     
    if max_x is None:
        max_x=(df.shape[0])
        
    if min_y is None:        
        calc=min(df[cols_to_bars].max(axis=1).tolist())-min(df[cols_to_bars].max(axis=1).tolist())*0.5
        min_y=calc if min(df[cols_to_bars].max(axis=1).tolist())<0 else 0
    
    if max_y is None:
        max_y=max(df[cols_to_bars].max(axis=1).tolist())+max(df[cols_to_bars].max(axis=1).tolist())*0.5
       
    ax1=df[cols_to_bars].plot(kind='bar', width = width, color=bars_colors)
    ax1.set_xticklabels(df[x_labels_col], rotation=rotation)
    
    #подписи 
    if label_bars:
        add_value_labels(ax1, 'bar', label_bars_add_str, label_bars_box)

    #убираем текст и метки с левой оси
    if not(y_axis_text):
        ax1.set_yticklabels([])
        ax1.tick_params(left=False)

    #mix, max y 
    ax1.set_ylim(min_y, max_y)

    #min, max x
    ax1.set_xlim(min_x, max_x)

    if not(show_legend):
        ax1.get_legend().remove()

    if plot_title is not None:
        plt.title(plot_title, loc='left')
   
    #настройка разрешения    
    if img_resolution is None:
        res=plt.gcf()
        res.set_dpi(dpi)
    else:
        res=plt.gcf()
        res.set_dpi(dpi)
        res.set_size_inches(img_resolution[0]/dpi, img_resolution[1]/dpi)
        
    if grid:
        plt.grid()
        
    return plt.gcf()
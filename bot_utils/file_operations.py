# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 16:21:25 2024

@author: John
"""

import os  # to use OS dependent functionality (in this case: delete image files)


def delete_images(df_image_filenames=None, plot_image_filenames=None):
    """
    Delete image files specified in the provided list(s).
    
    Parameters:
    - df_image_filenames (list): List of filenames for DataFrame image chunks.
    - plot_image_filenames (list): List of filenames for plot images.
    """
    # the function takes in a list of filenames and iterates through it
    # the files to be deleted in this block are chunks of a dataframe in png format
    if df_image_filenames:
        for filename in df_image_filenames:
            if os.path.exists(filename):
                os.remove(filename)
    
    # temporary solution. For now, the bot only returns one plot, so it's deleted manually.
    # When more plots are created, change the code accordingly -> (if plot_image_filenames: ...)
    plot_path = 'plot.png'
    if os.path.exists(plot_path):
        os.remove(plot_path)

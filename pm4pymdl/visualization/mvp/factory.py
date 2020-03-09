import os
import shutil
import subprocess
import sys
import traceback


from pm4pymdl.visualization.mvp.versions import classic

CLASSIC = "classic"

VERSIONS = {CLASSIC: classic.apply}


def apply(model, parameters=None, variant=CLASSIC):
    """
    Visualize StarStar models

    Parameters
    ------------
    model
        StarStar model
    parameters
        Possible parameters of the algorithm
    variant
        Variants available for the visualizer, possible values: classic

    Returns
    -----------
    file_name
        File name where a representation of the StarStar model is stored
    """
    return VERSIONS[variant](model, parameters=parameters)


def view(figure):
    """
    View on the screen a figure that has been rendered

    Parameters
    ----------
    figure
        figure
    """
    try:
        filename = figure.name
        figure = filename
    except AttributeError:
        # continue without problems, a proper path has been provided
        pass

    is_ipynb = False

    try:
        get_ipython()
        is_ipynb = True
    except NameError:
        pass

    if is_ipynb:
        from IPython.display import Image
        return Image(open(figure, "rb").read())
    else:
        if sys.platform.startswith('darwin'):
            subprocess.call(('open', figure))
        elif os.name == 'nt':  # For Windows
            os.startfile(figure)
        elif os.name == 'posix':  # For Linux, Mac, etc.
            subprocess.call(('xdg-open', figure))


def save(figure, output_file_path):
    """
    Save a figure that has been rendered

    Parameters
    -----------
    figure
        figure
    output_file_path
        Path where the figure should be saved
    """
    try:
        filename = figure.name
        figure = filename
    except AttributeError:
        # continue without problems, a proper path has been provided
        pass
        traceback.print_exc()

    shutil.copyfile(figure, output_file_path)

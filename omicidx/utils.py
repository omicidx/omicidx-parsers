import gzip

def open_file(fname, mode = 'rt'):
    """open a file, including dealing with gzipped files
 
    Parameters
    ----------
    fname : str
        a filename. If ending in .gz, will use gzip.open(). 
        Otherwise, a regular open() will be used.

    mode : str
        The file opening mode. 
    """
    if(fname.endswith('.gz')):
        f = gzip.open(fname, mode)
    else:
        f = open(fname, mode)
    return(f)

import warnings
import polars as pl
from pathlib import Path
from typing import Optional, Callable
from zipfile import ZipFile, is_zipfile
from tarfile import TarFile, is_tarfile

from tempfile import TemporaryDirectory


__all__ = [
    "read_tsv", "scan_tsv", "getreader", "load_data", "read_archive",
    "is_archive_file"
]

def read_tsv(filepath: Path, lazy: bool = False, **kwargs
) -> pl.DataFrame|pl.LazyFrame:
    """Read TSV file with optimized settings for polars.
    
    Args:
        filepath: Path to TSV file
        lazy: Whether to read lazily (default: False)
        **kwargs: Additional arguments passed to pl.read_csv
        
    Returns:
        pl.DataFrame: Loaded data frame
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or invalid
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"TSV file not found: {filepath}")
    
    if filepath.stat().st_size == 0:
        raise ValueError(f"TSV file is empty: {filepath}")
    
    try:
        if lazy:
            return pl.scan_csv(
                filepath,
                separator='\t',
                quote_char=None,
                **kwargs
            )
        else:
            return pl.read_csv(
                filepath,
                separator='\t',
                quote_char=None,
                **kwargs
            )
    except Exception as e:
        raise ValueError(f"Failed to read TSV file {filepath}: {e}")

def scan_tsv(filepath: Path, **kwargs) -> pl.LazyFrame:
    """Scan TSV file with optimized settings for polars.
    
    Args:
        filepath: Path to TSV file
        **kwargs: Additional arguments passed to pl.scan_csv
        
    Returns:
        pl.LazyFrame: Loaded lazy data frame
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or invalid
    """
    return read_tsv(filepath, lazy=True, **kwargs)


def _validate_input(file_path: Path | str, format_type: Optional[str]) -> tuple[Path, str]:
    """Validate and normalize input parameters.
    
    Args:
        file_path: Path to the file
        format_type: Optional format override
        
    Returns:
        tuple: (validated_path, format_string)
        
    Raises:
        TypeError: If file_path is invalid
        ValueError: If format_type is invalid
    """
    if not file_path:
        raise TypeError("file_path cannot be empty")
    
    try:
        validated_path = Path(file_path)
    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid file_path: {e}")
    
    if format_type:
        fmt = format_type.lower().strip()
        if not fmt:
            raise ValueError("format_type cannot be empty string")
    else:
        if not validated_path.suffix:
            raise ValueError(f"Cannot determine format from file path: {validated_path}")
        fmt = validated_path.suffix.lower().lstrip('.')
    
    return validated_path, fmt


def _get_reader_mapping(lazy: bool) -> dict[str, Callable]:
    """Get format-to-reader mapping based on lazy mode.
    
    Args:
        lazy: Whether to use lazy readers
        
    Returns:
        dict: Format to reader function mapping
    """
    if lazy:
        return {
            'csv': pl.scan_csv,
            'parquet': pl.scan_parquet,
            'ipc': pl.scan_ipc,
            'tsv': scan_tsv
        }
    
    return {
        'csv': pl.read_csv,
        'tsv': read_tsv,
        'xls': pl.read_excel,
        'xlsx': pl.read_excel,
        'ods': pl.read_ods,
        'json': pl.read_json,
        'parquet': pl.read_parquet,
        'ipc': pl.read_ipc,
        'avro': pl.read_avro,
    }


def _handle_focus_mode(fmt: str) -> Callable:
    """Handle focus mode with strict format checking.
    
    Args:
        fmt: Format string
        
    Returns:
        Callable: Reader function
        
    Raises:
        ValueError: If format is not supported
    """
    reader = getattr(pl, f"read_{fmt}", None)
    if reader is None:
        raise ValueError(
            f"Unsupported format '{fmt}' in focus mode. "
        )
    return reader


def _get_fallback_reader(lazy: bool) -> Callable:
    """Get fallback reader for unsupported formats.
    
    Args:
        lazy: Whether to use lazy reader
        
    Returns:
        Callable: Fallback reader function
    """
    return pl.scan_csv if lazy else pl.read_csv


def getreader(
    file_path: Path | str,
    format_type: Optional[str] = None,
    in_batch: bool = False,
    lazy: bool = False,
    focus: bool = False
) -> Callable:
    """Get appropriate reader function based on file extension or specified format.
    
    Args:
        file_path: Path to the file
        format_type: Optional format override (e.g., 'csv', 'json', 'parquet')
        in_batch: Whether to read in batch mode, only for csv file (default: False)
        lazy: Whether to read lazily, only for csv, ipc, parquet file (default: False)
        focus: Whether to focus on the specified format, if not supported, raise ValueError (default: False)
        
    Returns:
        Callable: Polars reader function for the specified format
        
    Raises:
        ValueError: If format is not supported and focus=True
        TypeError: If file_path is invalid
    """
    # Step 1: Validate input
    _, fmt = _validate_input(file_path, format_type)
    
    # Step 2: Handle special cases
    if focus:
        return _handle_focus_mode(fmt)
    
    if fmt == 'csv' and in_batch:
        return pl.read_csv_batched
    
    # Step 3: Get appropriate reader
    reader_mapping = _get_reader_mapping(lazy)
    
    # Step 4: Return reader or fallback
    if fmt in reader_mapping:
        return reader_mapping[fmt]
    
    # Fallback with warning
    supported = ', '.join(sorted(reader_mapping.keys()))
    warnings.warn(
        f"Unknown format '{fmt}', falling back to {'lazy' if lazy else 'eager'} CSV reader. "
        f"Supported formats: {supported}",
        UserWarning,
        stacklevel=2
    )
    return _get_fallback_reader(lazy)


def _is_archive_file(file_path: Path) -> bool:
    """
    Check if the file is an archive file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        bool: True if the file is an archive file(zip or tar), False otherwise
    """
    if not file_path.is_file():
        return False
    return is_zipfile(file_path) or is_tarfile(file_path)


def is_archive_file(file_path: Path) -> bool:
    """
    Check if the file is an archive file or the file is in an archive file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        bool: True if the file is an archive file(zip or tar), False otherwise
    """
    if not file_path.is_file():
        return False
    return _is_archive_file(file_path) or any(
        _is_archive_file(p) for p in file_path.parents
    )


def read_archive(file_path: str | Path,
                 filename: Optional[str] = None,
                 format_type: Optional[str] = None,
                 **kwargs
) -> pl.DataFrame :
    """
    Read data file inside a zip file or a tar file.
    
    The Path like : `path/to/compressed.zip/data.csv` or `path/to/compressed.tar.gz`

    Args:
        file_path: Path to data file inside the zip file or tar file
        filename: Optional filename to read from the zip file, if None, read the first file in the zip file
        format_type: Optional format override (e.g., 'csv', 'json', 'parquet')
        **kwargs: Additional arguments to pass to the reader function
        
    Returns:
        pl.DataFrame: Loaded data
    """
    file_path = Path(file_path)
    
    # Step 1: Determine archive path and filename
    archive_path, target_filename = _resolve_archive_and_filename(file_path, filename)
    
    # Step 2: Get appropriate reader function
    focus = format_type is not None
    reader = getreader(target_filename, format_type=format_type, focus=focus)
    
    # Step 3: Read data from archive
    return _read_from_archive(archive_path, target_filename, reader, **kwargs)


def _resolve_archive_and_filename(file_path: Path, filename: Optional[str]) -> tuple[Path, str]:
    """
    Resolve archive path and target filename from the given file path.
    
    Args:
        file_path: Path to data file inside archive
        filename: Optional filename override
        
    Returns:
        tuple: (archive_path, target_filename)
        
    Raises:
        ValueError: If file_path is not inside an archive file
    """
    if filename is not None:
        # If filename is provided, archive_path is the parent directory
        archive_path = file_path.parent
        if not _is_archive_file(archive_path):
            raise ValueError(f"Parent directory is not an archive file: {archive_path}")
        return archive_path, filename
    
    # Case 1: Parent directory is an archive
    if _is_archive_file(file_path.parent):
        return file_path.parent, file_path.name
    
    # Case 2: File itself is an archive
    if _is_archive_file(file_path):
        return file_path, _get_archive_filename(file_path)
    
    # Case 3: Archive is in parent directories
    for parent in file_path.parents:
        if _is_archive_file(parent):
            return parent, file_path.name
    
    raise ValueError("file_path must be a file inside an archive file (zip or tar)")


def _read_from_archive(archive_path: Path, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    Read data from archive file using the specified reader function.
    
    Args:
        archive_path: Path to archive file
        filename: Name of file to read inside archive
        reader: Reader function to use
        **kwargs: Additional arguments for reader
        
    Returns:
        pl.DataFrame: Loaded data
    """
    if is_zipfile(archive_path):
        return _read_from_zip(archive_path, filename, reader, **kwargs)
    elif is_tarfile(archive_path):
        return _read_from_tar(archive_path, filename, reader, **kwargs)
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")


def _read_from_zip(archive_path: Path, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    Read data from zip archive.
    
    Args:
        archive_path: Path to zip file
        filename: Name of file to read inside zip
        reader: Reader function to use
        **kwargs: Additional arguments for reader
        
    Returns:
        pl.DataFrame: Loaded data
    """
    with ZipFile(archive_path, 'r') as zf:
        try:
            # Try to read directly from archive
            with zf.open(filename) as f:
                return reader(f, **kwargs)
        except Exception:
            # Fallback: extract to temporary directory
            return _extract_and_read_zip(zf, filename, reader, **kwargs)


def _read_from_tar(archive_path: Path, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    Read data from tar archive.
    
    Args:
        archive_path: Path to tar file
        filename: Name of file to read inside tar
        reader: Reader function to use
        **kwargs: Additional arguments for reader
        
    Returns:
        pl.DataFrame: Loaded data
    """
    with TarFile(archive_path, 'r:*') as tf:
        try:
            # Try to read directly from archive
            with tf.extractfile(filename) as f:
                return reader(f, **kwargs)
        except Exception:
            # Fallback: extract to temporary directory
            return _extract_and_read_tar(tf, filename, reader, **kwargs)


def _extract_and_read_zip(zf: ZipFile, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    Extract file from zip and read it.
    
    Args:
        zf: ZipFile object
        filename: Name of file to extract and read
        reader: Reader function to use
        **kwargs: Additional arguments for reader
        
    Returns:
        pl.DataFrame: Loaded data
    """
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / filename
        zf.extract(filename, tmpdir)
        return reader(tmp_path, **kwargs)


def _extract_and_read_tar(tf: TarFile, filename: str, reader: Callable, **kwargs) -> pl.DataFrame:
    """
    Extract file from tar and read it.
    
    Args:
        tf: TarFile object
        filename: Name of file to extract and read
        reader: Reader function to use
        **kwargs: Additional arguments for reader
        
    Returns:
        pl.DataFrame: Loaded data
    """
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / filename
        
        # Use safe extraction if available
        if hasattr(tarfile, 'data_filter'):
            tf.extract(filename, tmpdir, filter='data')
        else:
            warnings.warn(
                "Extracting may be unsafe; consider updating Python",
                UserWarning,
                stacklevel=2
            )
            tf.extract(filename, tmpdir)
        
        return reader(tmp_path, **kwargs)



def load_data(
    file_path: Path | str,
    format_type: Optional[str] = None,
    in_batch: bool = False,
    lazy: bool = False,
    focus: bool = False,
    transtype: pl.Expr|list[pl.Expr]|None = None,
    **kwargs
) -> pl.DataFrame | pl.LazyFrame | pl.BatchedCsvReader:
    """
    Load data from a file using the appropriate reader.

    Args:
        file_path: Path to the file
        format_type: Optional format override (e.g., 'csv', 'json', 'parquet')
        in_batch: Whether to read in batch mode, only for csv file (default: False)
        lazy: Whether to read lazily, only for csv, ipc, parquet file (default: False)
        focus: Whether to focus on the specified format, if not supported, raise ValueError (default: False)
        transtype: Optional type transformation expression(s) to apply to the data
        **kwargs: Additional arguments to pass to the reader function
        
    Returns:
        pl.DataFrame | pl.LazyFrame | pl.BatchedCsvReader: Loaded data
    """
    # Step 1 : load data
    if is_archive_file(file_path):
        df = read_archive(file_path, format_type=format_type, **kwargs)
    else:
        reader = getreader(
            file_path,
            format_type=format_type,
            in_batch=in_batch,
            lazy=lazy,
            focus=focus
        )
        df = reader(file_path, **kwargs)
    
    # Step 2 : apply type transformation
    if transtype is not None:
        if isinstance(transtype, pl.Expr):
            df = df.with_columns(transtype)
        elif isinstance(transtype, list):
            df = df.with_columns(*transtype)
        else:
            raise ValueError("transtype must be a polars expression or a list of polars expressions")
    
    return df
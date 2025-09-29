import warnings
import polars as pl
from pathlib import Path
from typing import Optional, Callable

__all__ = [
    "read_tsv", "scan_tsv", "getreader", "load_data"
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
    # Step 1 : get reader function
    reader = getreader(
        file_path,
        format_type=format_type,
        in_batch=in_batch,
        lazy=lazy,
        focus=focus
    )

    # Step 2 : load data
    df = reader(file_path, **kwargs)
    
    # Step 3 : apply type transformation
    if transtype is not None:
        if isinstance(transtype, pl.Expr):
            df = df.with_columns(transtype)
        elif isinstance(transtype, list):
            df = df.with_columns(*transtype)
        else:
            raise ValueError("transtype must be a polars expression or a list of polars expressions")
    
    return df
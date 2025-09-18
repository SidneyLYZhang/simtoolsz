from typing import Optional
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import duckdb


def zip2db(zip_file: Path, db_file: Path, 
           filename: Optional[str] = None,
           **kwargs
) -> None :
    with TemporaryDirectory() as tmpdir:
        with ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        if filename :
            xfile = Path(tmpdir) / filename
        else :
            xfile = Path(tmpdir) / "*.*"
        with duckdb.connect(db_file) as con :
            con.sql(f"COPY FROM '{xfile}'")
            pass

    pass

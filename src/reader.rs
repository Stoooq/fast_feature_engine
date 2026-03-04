use polars::prelude::*;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;

pub struct BatchReader {
    file_list: std::vec::IntoIter<PathBuf>,
    current_file_path: Option<PathBuf>,
    current_file_offset: usize,
    previous_tail: Option<DataFrame>,

    saved_columns: Option<Vec<String>>,

    batch_size: usize,
    tail_size: usize,
}

impl BatchReader {
    pub fn new(directory_path: &str, batch_size: usize, tail_size: usize) -> Self {
        let mut csv_files: Vec<PathBuf> = fs::read_dir(directory_path)
            .expect("Can't read directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| path.extension() == Some(OsStr::new("csv")))
            .collect();

        csv_files.sort_by_key(|path| {
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

            let parts: Vec<&str> = stem.split("-").collect();
            if parts.len() >= 2 {
                format!("{}-{}", parts[parts.len() - 2], parts[parts.len() - 1])
            } else {
                stem.to_string()
            }
        });

        println!("{:?}", csv_files);

        BatchReader {
            file_list: csv_files.into_iter(),
            current_file_path: None,
            current_file_offset: 0,
            previous_tail: None,
            saved_columns: None,
            batch_size,
            tail_size,
        }
    }
}

impl Iterator for BatchReader {
    type Item = DataFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunks_to_concat: Vec<DataFrame> = Vec::new();
        let mut collected_rows = 0;

        while collected_rows < self.batch_size {
            if self.current_file_path.is_none() {
                self.current_file_path = self.file_list.next();
                self.current_file_offset = 0;
            }

            if let Some(path) = &self.current_file_path {
                let rows_needed = self.batch_size - collected_rows;

                let df = if self.current_file_offset == 0 {
                    let loaded_df = CsvReadOptions::default()
                        .with_has_header(true)
                        .with_n_rows(Some(rows_needed))
                        .try_into_reader_with_file_path(Some(path.clone()))
                        .unwrap()
                        .finish()
                        .unwrap();

                    if self.saved_columns.is_none() {
                        let cols = loaded_df.get_column_names()
                            .into_iter()
                            .map(|c| c.to_string())
                            .collect();
                        self.saved_columns = Some(cols);
                    }
                    loaded_df
                } else {
                    let skip_lines = self.current_file_offset + 1;
                    let mut loaded_df = CsvReadOptions::default()
                        .with_has_header(false)
                        .with_skip_rows(skip_lines)
                        .with_n_rows(Some(rows_needed))
                        .try_into_reader_with_file_path(Some(path.clone()))
                        .unwrap()
                        .finish()
                        .unwrap();

                    if let Some(cols) = &self.saved_columns {
                        let cols_str: Vec<&str> = cols.iter().map(|c| c.as_str()).collect();
                        loaded_df.set_column_names(&cols_str).unwrap();
                    }
                    loaded_df
                };

                let rows_read = df.height();

                if rows_read > 0 {
                    chunks_to_concat.push(df);
                    collected_rows += rows_read;
                    self.current_file_offset += rows_read;
                }

                if rows_read < rows_needed {
                    self.current_file_path = None;
                }
            } else {
                break;
            }
        }

        if chunks_to_concat.is_empty() {
            return None;
        }

        let lazy_chunks: Vec<LazyFrame> =
            chunks_to_concat.into_iter().map(|df| df.lazy()).collect();

        let mut raw_batch = concat(lazy_chunks, UnionArgs::default())
            .unwrap()
            .collect()
            .unwrap();

        if let Some(tail) = &self.previous_tail {
            raw_batch = tail.vstack(&raw_batch).unwrap();
        }

        self.previous_tail = Some(raw_batch.tail(Some(self.tail_size)));

        Some(raw_batch)
    }
}

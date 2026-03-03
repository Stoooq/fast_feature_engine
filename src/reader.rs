use polars::prelude::*;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

pub struct BatchReader {
    file_list: std::vec::IntoIter<PathBuf>,
    current_df: Option<DataFrame>,
    current_offset: usize,
    previous_tail: Option<DataFrame>,

    batch_size: usize,
    tail_size: usize,
}

impl BatchReader {
    pub fn new(directory_path: &str, batch_size: usize, tail_size: usize) -> Self {
        let csv_files: Vec<PathBuf> = fs::read_dir(directory_path)
            .expect("Can't read directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| path.extension() == Some(OsStr::new("csv")))
            .collect();

        BatchReader {
            file_list: csv_files.into_iter(),
            current_df: None,
            current_offset: 0,
            previous_tail: None,
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
            if self.current_df.is_none() {
                if let Some(path) = self.file_list.next() {
                    let df = CsvReadOptions::default()
                        .with_n_rows(Some(self.batch_size))
                        .try_into_reader_with_file_path(Some(path))
                        .unwrap()
                        .finish()
                        .unwrap();

                    self.current_df = Some(df);
                    self.current_offset = 0;
                } else {
                    break;
                }
            }

            if let Some(df) = &self.current_df {
                let rows_needed = self.batch_size - collected_rows;
                let rows_available = df.height() - self.current_offset;
                let rows_to_take = std::cmp::min(rows_needed, rows_available);

                if rows_to_take > 0 {
                    let chunk = df.slice(self.current_offset as i64, rows_to_take);
                    chunks_to_concat.push(chunk);
                    collected_rows += rows_to_take;
                    self.current_offset += rows_to_take;
                }

                if self.current_offset >= df.height() {
                    self.current_df = None;
                }
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

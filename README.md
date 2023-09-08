# Common Crawl Data Extractor

This Python script is designed to download and extract information from Common Crawl data files, specifically WARC files containing robots.txt data. It performs the following tasks:

1. Downloads a WARC file from Common Crawl's data archive.
2. Extracts information about user-agents, disallowed paths, and allowed paths for each domain from the WARC file.
3. Stores the extracted data in a CSV file.
4. Generates statistics on the extracted data and stores the statistics in a separate CSV file.

## Prerequisites

Before running the script, ensure you have the following dependencies installed:

- Python 3.x
- pandas
- requests
- warcio
- gzip

You can install the required Python packages using pip:

```pip install <package_name>```

## Usage

1. Clone this repository to your local machine.

2. Open a terminal and navigate to the project directory.

3. Run the script with the following command:
```python run.py```

The script will perform the following steps:

- Download the WARC file from Common Crawl.
- Extract data from the WARC file.
- Create a CSV file named `output.csv` containing the extracted data.
- Generate statistics and save them in a CSV file named `stats.csv`.

## Output Files
The script generates two output files:
- output.csv: This CSV file contains the extracted data, including fetched date, domain, HTTP status code, user agent, disallow count, and allow count.
- stats.csv: This CSV file provides statistics about the daily runs, including date, total errors (non-200 status codes), total successful responses (status code 200), total distinct user agents, total allowed paths, and total disallowed paths.

## Why CSV Format?

CSV (Comma-Separated Values) format was chosen for storing the extracted data due to several advantages:

- **Human-Readable:** CSV files are inherently human-readable. They consist of plain text data separated by commas, making them easily understandable without specialized software. You can open and view CSV files using a wide range of applications, including spreadsheet software like Microsoft Excel, Google Sheets, or LibreOffice Calc.

- **Data Integrity:** CSV files maintain the integrity and structure of the data. Each row represents a record, and columns represent different attributes or fields. This structure ensures that the data remains organized and coherent, which is essential for reliable analysis.

- **Compatibility:** CSV is a universally supported format in the data analysis and visualization ecosystem. It seamlessly integrates with various data science libraries, such as Pandas, NumPy, and Matplotlib. This compatibility allows data scientists and analysts to import CSV data directly into their preferred tools for exploration and visualization.

- **Scalability:** CSV files are suitable for datasets of varying sizes. Whether you're working with small datasets or large datasets containing millions of records, CSV remains a versatile and efficient choice for data storage. This scalability ensures that the format can accommodate different project requirements and data volumes.


## Configuration

You can customize the script by modifying the following variables in the `run.py` file:

- `input_file`: The path to the folder where the WARC file will be downloaded and stored.
- `output_file_path`: The path to the folder where the output CSV file will be saved.
- `stats_file_path`: The path to the folder where the statistics CSV file will be saved.

## Acknowledgments

- Common Crawl for providing access to their data.
- The Python community for creating useful libraries and tools.

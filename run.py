from warcio.archiveiterator import ArchiveIterator
import pandas as pd
from collections import defaultdict
import requests
import gzip
from io import BytesIO


class Extractor:

    def __init__(self, input_file, out_file_path, stats_file_path):
        self.input_file = input_file
        self.output_file_path = out_file_path
        self.stats_file_path = stats_file_path
        self.output_df = pd.DataFrame(None)
        self.response_data = []
        self.non200_count = 0
        self.ok200_count = 0

    def download_file(self):
        URL = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296950528.96/robotstxt/CC-MAIN-20230402105054-20230402135054-00799.warc.gz'
        file_path = f'{self.input_file}/input.warc'

        response = requests.get(URL)
        if response.status_code == 200:
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as decompressed_file:
                # Read and save the decompressed content to the local path
                with open(file_path, 'wb') as output_file:
                    output_file.write(decompressed_file.read())
            print(f"File downloaded and decompressed to: {file_path}")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")

    def warc_extraction(self):
        # Read the WARC file.
        with open(f'{self.input_file}/input.warc', 'rb') as f:
            for record in ArchiveIterator(f):
                user_agent = None
                disallow_count = 0
                allow_count = 0

                if record.rec_type == 'response':

                    # Dictionary to insert every record in WARC file.
                    response_item = defaultdict()
                    content = record.content_stream().read().decode('utf-8', 'ignore')
                    response_item['fetched_date'] = record.rec_headers.get_header('WARC-Date')
                    response_item['domain'] = record.rec_headers.get_header('WARC-Target-URI').split('/')[2]
                    response_item['http_code'] = int(record.http_headers.get_statuscode())

                    # If status code is 200 then count allowed and disallowed.
                    if response_item['http_code'] == 200:
                        # count the status code 200 ok for later storing in statistic file.
                        self.ok200_count += 1
                        response_item['agents'] = []
                        for line in content.split('\n'):

                            if line.startswith('User-agent:'):
                                if user_agent != 'None':
                                    response_item['agents'].append(
                                        {'user_agent': user_agent, 'disallow_count': disallow_count,
                                         'allow_count': allow_count})
                                    # If there are multiple user_agents for the domain, restore count values.
                                    disallow_count = 0
                                    allow_count = 0
                                # Using strip() to remove white spaces.
                                user_agent = line.split(':')[1].strip()

                            elif line.startswith('Disallow:'):
                                disallow_count += 1

                            elif line.startswith('Allow:'):
                                allow_count += 1
                        # If there are multiple user_agents then to store the last one.
                        response_item['agents'].append(
                            {'user_agent': user_agent, 'disallow_count': disallow_count, 'allow_count': allow_count})
                    else:
                        # Status code count other than 200.
                        self.non200_count += 1
                    self.response_data.append(response_item)

    def create_output_csv(self):
        fetched_dates = []
        domains = []
        http_codes = []
        user_agents = []
        disallow_counts = []
        allow_counts = []

        for item in self.response_data:
            fetched_date = item['fetched_date']
            domain = item['domain']
            http_code = item['http_code']
            if 'agents' in item:
                agents = item['agents']
                for agent in agents:
                    user_agent = agent['user_agent']
                    disallow_count = agent.get('disallow_count', 0)
                    allow_count = agent.get('allow_count', 0)

                    fetched_dates.append(fetched_date)
                    domains.append(domain)
                    http_codes.append(http_code)
                    user_agents.append(user_agent)
                    disallow_counts.append(disallow_count)
                    allow_counts.append(allow_count)
            else:
                fetched_dates.append(fetched_date)
                domains.append(domain)
                http_codes.append(http_code)
                user_agents.append(None)
                disallow_counts.append(None)
                allow_counts.append(None)

        response_dictionary = {
            'fetched_date': fetched_dates,
            'domain': domains,
            'http_code': http_codes,
            'user_agent': user_agents,
            'disallow_count': disallow_counts,
            'allow_count': allow_counts
        }

        self.output_df = pd.DataFrame(response_dictionary)

        self.output_df.to_csv(f'{self.output_file_path}/output.csv')

    def create_stats_csv(self):
        # Convert the fetched date into dateformat as per pandas.
        self.output_df['fetched_date'] = pd.to_datetime(self.output_df['fetched_date'])

        stats_df = pd.DataFrame(
            columns=['date', 'total_errors', 'total_ok', 'total_distinct_ua', 'total_allows', 'total_disallows'])

        # Group by the date for further analysis.
        grouped_df = self.output_df.groupby(self.output_df['fetched_date'].dt.date)

        for date, record in grouped_df:
            total_distinct_ua = record['user_agent'].nunique()
            total_allows = record['allow_count'].sum()
            total_disallows = record['disallow_count'].sum()

            stats_df = stats_df.append({
                'date': date,
                'total_errors': self.non200_count,
                'total_ok': self.ok200_count,
                'total_distinct_ua': total_distinct_ua,
                'total_allows': total_allows,
                'total_disallows': total_disallows
            }, ignore_index=True)

        stats_df.to_csv(f'{self.stats_file_path}/stats.csv')


def main():
    input_file = 'data/raw'
    output_file = 'data/extracted'
    statistic_file = 'data/statistics'
    e = Extractor(input_file, output_file, statistic_file)

    # Download the warc from the URL mentioned.
    e.download_file()
    # Extracts the data from warc file.
    e.warc_extraction()
    # Creates a dataframe of the extracted data to store it in csv file.
    e.create_output_csv()
    # Creates csv file post analysis done on the extracted data.
    e.create_stats_csv()

    # Chose csv file format for data storage for easy data access. It seamlessly integrates with pandas library for any further data analysis.


if __name__ == '__main__':
    main()

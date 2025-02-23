import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import os
from datetime import datetime
import schedule
import sys
import concurrent.futures
import threading
from queue import Queue
from wikipedia_updater import *
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

class PlayerTracker:
    def __init__(self):
        self.tracker_file = "player_tracker.json"
        self.processed_file = "processed_players.json"
        self.failed_file = "failed.csv"  # New file for failed players
        self.lock = threading.Lock()
        self.load_tracker()
        
    def load_tracker(self):
        try:
            if os.path.exists(self.tracker_file):
                with open(self.tracker_file, 'r') as f:
                    self.tracker = json.load(f)
            else:
                self.tracker = {"total_players": 0, "processed_count": 0}
                
            if os.path.exists(self.processed_file):
                with open(self.processed_file, 'r') as f:
                    self.processed_players = json.load(f)
            else:
                self.processed_players = []
                
            # Initialize failed players tracking
            if os.path.exists(self.failed_file):
                self.failed_players = pd.read_csv(self.failed_file)['Player Name'].tolist()
            else:
                self.failed_players = []
                pd.DataFrame(columns=['Player Name']).to_csv(self.failed_file, index=False)
                
        except Exception as e:
            logging.error(f"Error loading tracker files: {str(e)}")
            self.tracker = {"total_players": 0, "processed_count": 0}
            self.processed_players = []
            self.failed_players = []
            
    def save_tracker(self):
        try:
            with open(self.tracker_file, 'w') as f:
                json.dump(self.tracker, f)
        except Exception as e:
            logging.error(f"Error saving tracker file: {str(e)}")
            
    def save_processed_players(self):
        try:
            with open(self.processed_file, 'w') as f:
                json.dump(self.processed_players, f)
        except Exception as e:
            logging.error(f"Error saving processed players file: {str(e)}")
            
    def reset_tracker(self):
        with self.lock:
            self.tracker["processed_count"] = 0
            self.processed_players = []
            self.save_tracker()
            self.save_processed_players()

    def add_processed_player(self, player_name):
        if player_name in self.processed_players:
            return
            
        with self.lock:
            if player_name not in self.processed_players:
                self.processed_players.append(player_name)
                self.tracker["processed_count"] += 1
        
        self.save_processed_players()
        self.save_tracker()

    def add_failed_player(self, player_name):
        with self.lock:
            if player_name not in self.failed_players:
                self.failed_players.append(player_name)
                pd.DataFrame({'Player Name': [player_name]}).to_csv(
                    self.failed_file, 
                    mode='a', 
                    header=False, 
                    index=False
                )
                logging.error(f"Added {player_name} to failed players list after multiple retries")

def process_player(player, tracker, wiki_site, max_retries=4):
    player_name = player['Player Name']
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Processing {player_name}... (Attempt {attempt + 1}/{max_retries})")
            
            # Add timeout to requests
            stats_df, averages_df, dob = extract_tables_data(player['Profile Link'])
            if stats_df is None or averages_df is None:
                raise Exception("Failed to extract data")
                
            stats_df, total_career_df, votes_df, averages_df = process_player_stats(stats_df, averages_df)
            if stats_df is None or averages_df is None:
                raise Exception("Failed to process stats")
                
            json_output = convert_dataframes_to_json(stats_df, total_career_df, votes_df, averages_df)
            success = update_wikipedia_page(player_name, json_output, wiki_site, dob)
            
            if success:
                tracker.add_processed_player(player_name)
                logging.info(f"Successfully processed {player_name}")
                return True
            else:
                raise Exception("Failed to update Wikipedia page")
            
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed for {player_name}: {str(e)}")
            time.sleep(5)  # Wait between retries
            
            if attempt == max_retries - 1:
                logging.error(f"All attempts failed for {player_name}")
                tracker.add_failed_player(player_name)
                return False
                
        finally:
            time.sleep(3)  # Rate limiting between attempts

def process_players_thread(players_chunk, tracker, wiki_site):
    for player in players_chunk:
        process_player(player, tracker, wiki_site)

def extract_tables_data(url, timeout=30):
    website_columns_mapping = {
        "Year": "Season",
        "Team": "Team",
        "#": "No.",
        "GM": "Games",
        "GL": "G",
        "BH": "B",
        "KI": "K",
        "HB": "H",
        "DI": "D",
        "MK": "M",
        "TK": "T",
        "BR": "Votes"
    }
    
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        html_content = response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return None, None, None

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tabs_content = soup.find_all('div', class_='simpleTabsContent')
        dataframes = []
        
        for tab_content in tabs_content:
            table = tab_content.find('table', {'class': 'sortable'})
            if table:
                headers = []
                for th in table.find('thead').find_all('th'):
                    header = th.find('a')
                    headers.append(header.text if header else th.text.strip())
                
                rows = []
                tbody = table.find('tbody')
                if tbody:
                    for tr in tbody.find_all('tr'):
                        row = []
                        for td in tr.find_all('td'):
                            cell = td.find('a')
                            row.append(cell.text if cell else td.text.strip())
                        rows.append(row)
                
                footer_rows = []
                tfoot = table.find('tfoot')
                if tfoot:
                    for tr in tfoot.find_all('tr'):
                        footer_row = []
                        for td in tr.find_all('td'):
                            footer_row.append(td.text.strip())
                        footer_rows.append(footer_row)
                
                df = pd.DataFrame(rows, columns=headers)
                columns_to_keep = ["Year", "Team", "#", "GM", "GL", "BH", "KI", "HB", "DI", "MK", "TK", "BR"]
                filtered_df = df[columns_to_keep]
                filtered_df = filtered_df.rename(columns=website_columns_mapping)
                
                stats_columns = ["GM", "GL", "BH", "KI", "HB", "DI", "MK", "TK", "BR"]
                mapped_stats_columns = [website_columns_mapping[col] for col in stats_columns]
                column_indices = [1, 7, 8, 3, 5, 6, 4, 10, 17]
                
                stats_df = pd.DataFrame(index=['Totals', 'Averages'], columns=filtered_df.columns)
                stats_df[['Season', 'Team', 'No.']] = ''
                
                for stat_col, mapped_col, idx in zip(stats_columns, mapped_stats_columns, column_indices):
                    stats_df.loc['Totals', website_columns_mapping[stat_col]] = footer_rows[0][idx].replace('b', '').strip()
                    stats_df.loc['Averages', website_columns_mapping[stat_col]] = footer_rows[1][idx].strip()
                
                final_df = pd.concat([filtered_df, stats_df])
                dataframes.append(final_df)
                dob = get_player_dob(soup)

        return tuple(dataframes) + (dob,) if dataframes else (None, None, None)
    except Exception as e:
        logging.error(f"Error parsing HTML from {url}: {str(e)}")
        return None, None, None

def get_user_inputs():
    while True:
        try:
            days = int(input("Enter how often to run the scraper (in days): "))
            if days <= 0:
                print("Please enter a positive number of days")
                continue
                
            year = int(input("Enter the year to scrape (e.g., 2024): "))
            if year < 1897 or year > datetime.now().year:
                print(f"Please enter a valid year between 1897 and {datetime.now().year}")
                continue
            
            thread_count = int(input("Enter the number of players you want to update at same time (1-10): "))
            if thread_count <= 0 or thread_count > 20:
                print("Please enter a number between 1 and 20")
                continue
                
            return days, year, thread_count
        except ValueError:
            print("Please enter valid numbers")

def run_scraper(year, thread_count):
    logging.info(f"Running scraper for year {year} with {thread_count} threads")
    
    wiki_site = initialize_apis()
    tracker = PlayerTracker()
    
    url = f"https://afltables.com/afl/stats/{year}.html"
    base_url = "https://afltables.com/afl/stats/"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all("table", class_="sortable")
        players_data = []
        
        for table in tables:
            for row in table.find_all("tr")[1:]:
                columns = row.find_all("td")
                if len(columns) > 1:
                    player_cell = columns[1]
                    if player_cell and player_cell.find("a"):
                        player_link = player_cell.find("a")["href"]
                        player_name = player_link.split("/")[-1].replace(".html", "")
                        
                        if player_name not in tracker.processed_players:
                            full_link = base_url + player_link
                            players_data.append({"Player Name": player_name, "Profile Link": full_link})
        
        tracker.tracker["total_players"] = len(players_data)
        tracker.save_tracker()
        
        # First pass - process all players
        chunk_size = max(1, len(players_data) // thread_count)
        players_chunks = [players_data[i:i + chunk_size] for i in range(0, len(players_data), chunk_size)]
        
        logging.info("Starting first pass...")
        process_chunks_with_executor(players_chunks, tracker, wiki_site, thread_count)
        
        # Second pass - retry failed players
        if tracker.failed_players:
            logging.info(f"Starting second pass for {len(tracker.failed_players)} failed players...")
            failed_players_data = [
                p for p in players_data 
                if p['Player Name'] in tracker.failed_players
            ]
            
            # Clear failed players list before second pass
            tracker.failed_players = []
            pd.DataFrame(columns=['Player Name']).to_csv(tracker.failed_file, index=False)
            
            failed_chunks = [failed_players_data[i:i + chunk_size] 
                           for i in range(0, len(failed_players_data), chunk_size)]
            
            process_chunks_with_executor(failed_chunks, tracker, wiki_site, thread_count)
        
        if tracker.tracker["processed_count"] >= tracker.tracker["total_players"]:
            logging.info("All players processed. Resetting tracker...")
            tracker.reset_tracker()
            
    except Exception as e:
        logging.error(f"Error in run_scraper: {str(e)}")

def process_chunks_with_executor(chunks, tracker, wiki_site, thread_count):
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = []
        for chunk in chunks:
            future = executor.submit(process_players_thread, chunk, tracker, wiki_site)
            futures.append(future)
        
        try:
            done, not_done = concurrent.futures.wait(
                futures, 
                timeout=3600,  # 1 hour timeout
                return_when=concurrent.futures.ALL_COMPLETED
            )
            
            # Cancel any incomplete futures
            for future in not_done:
                future.cancel()
                
            # Check for exceptions
            for future in done:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Thread error: {str(e)}")
                    
        except concurrent.futures.TimeoutError:
            logging.error("Processing timed out after 1 hour")
            
        finally:
            executor.shutdown(wait=False)

def convert_dataframes_to_json(stats_df, total_career_df, votes_df, averages_df):
    data_dict = {
        "stats_df": stats_df.to_dict(orient="records"),
        "averages_df": averages_df.to_dict(orient="records"),
        "votes_df": votes_df.to_dict(orient="records"),
        "total_career_df": total_career_df.to_dict(orient="records")
    }
    return data_dict

def get_player_dob(soup):
    try:
        born_element = soup.find(string=lambda text: "Born:" in text if text else False)
        if born_element:
            dob_element = born_element.find_next(string=True)
            if dob_element:
                dob = dob_element.strip()
                dob = dob.split('(')[0].strip()
                return dob
        return None
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"Error parsing data: {e}")
        return None

def schedule_scraper():
    days, year, thread_count = get_user_inputs()
    
    def job():
        run_scraper(year, thread_count)
    
    logging.info(f"Running first scrape for year {year} with {thread_count} threads")
    run_scraper(year, thread_count)
    
    schedule.every(days).days.do(job)
    
    logging.info(f"\nFirst scrape completed. Next run will be in {days} days.")
    logging.info("Press Ctrl+C to stop the scheduler")
    
    next_run = datetime.now() + pd.Timedelta(days=days)
    logging.info(f"Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            logging.info("\nScheduler stopped by user")
            sys.exit()

if __name__ == "__main__":
    os.makedirs("player_data", exist_ok=True)
    schedule_scraper()
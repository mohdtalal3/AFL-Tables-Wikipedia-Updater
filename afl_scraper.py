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

class PlayerTracker:
    def __init__(self):
        self.tracker_file = "player_tracker.json"
        self.processed_file = "processed_players.json"
        self.lock = threading.Lock()
        self.load_tracker()
        
    def load_tracker(self):
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
            
    def save_tracker(self):
        with self.lock:
            with open(self.tracker_file, 'w') as f:
                json.dump(self.tracker, f)
            
    def save_processed_players(self):
        with self.lock:
            with open(self.processed_file, 'w') as f:
                json.dump(self.processed_players, f)
            
    def reset_tracker(self):
        with self.lock:
            self.tracker["processed_count"] = 0
            self.processed_players = []
            self.save_tracker()
            self.save_processed_players()

    def add_processed_player(self, player_name):
        with self.lock:
            if player_name not in self.processed_players:
                self.processed_players.append(player_name)
                self.tracker["processed_count"] += 1
                self.save_processed_players()
                self.save_tracker()

def process_player(player, tracker, wiki_site):
    try:
        print(f"Processing {player['Player Name']}...")
        stats_df, averages_df, dob = extract_tables_data(player['Profile Link'])
        stats_df, total_career_df, votes_df, averages_df = process_player_stats(stats_df, averages_df)
        if stats_df is not None and averages_df is not None:
            json_output = convert_dataframes_to_json(stats_df, total_career_df, votes_df, averages_df)
            success = update_wikipedia_page(f"{player['Player Name']}", json_output, wiki_site, dob)
            
            if success:
                tracker.add_processed_player(player["Player Name"])
                print(f"Successfully processed {player['Player Name']}")
            
        time.sleep(3)
        
    except Exception as e:
        print(f"Error processing {player['Player Name']}: {str(e)}")

def process_players_thread(players_chunk, tracker, wiki_site):
    for player in players_chunk:
        process_player(player, tracker, wiki_site)

def extract_tables_data(url):
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
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.text
    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None, None, None

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
            
            thread_count = int(input("Enter the number of threads to use (1-20): "))
            if thread_count <= 0 or thread_count > 20:
                print("Please enter a number between 1 and 20")
                continue
                
            return days, year, thread_count
        except ValueError:
            print("Please enter valid numbers")

def run_scraper(year, thread_count):
    print(f"Running scraper for year {year} with {thread_count} threads")
    
    wiki_site = initialize_apis()
    tracker = PlayerTracker()
    
    url = f"https://afltables.com/afl/stats/{year}.html"
    base_url = "https://afltables.com/afl/stats/"
    
    response = requests.get(url)
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
    
    chunk_size = max(1, len(players_data) // thread_count)
    players_chunks = [players_data[i:i + chunk_size] for i in range(0, len(players_data), chunk_size)]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = []
        for chunk in players_chunks:
            future = executor.submit(process_players_thread, chunk, tracker, wiki_site)
            futures.append(future)
        
        concurrent.futures.wait(futures)
    
    if tracker.tracker["processed_count"] >= tracker.tracker["total_players"]:
        print("All players processed. Resetting tracker...")
        tracker.reset_tracker()

def schedule_scraper():
    days, year, thread_count = get_user_inputs()
    
    def job():
        run_scraper(year, thread_count)
    
    print(f"Running first scrape for year {year} with {thread_count} threads")
    run_scraper(year, thread_count)
    
    schedule.every(days).days.do(job)
    
    print(f"\nFirst scrape completed. Next run will be in {days} days.")
    print("Press Ctrl+C to stop the scheduler")
    
    next_run = datetime.now() + pd.Timedelta(days=days)
    print(f"Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            print("\nScheduler stopped by user")
            sys.exit()

if __name__ == "__main__":
    os.makedirs("player_data", exist_ok=True)
    schedule_scraper()
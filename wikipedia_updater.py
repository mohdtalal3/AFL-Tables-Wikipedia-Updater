from pywikibot import config
import pywikibot
from openai import OpenAI
from dotenv import load_dotenv
import os
import json
from datetime import datetime
import pandas as pd

import re
def update_or_insert_statistics_section_in_wikitext(old_wikitext, new_stats_markup):
    """
    Updates the existing statistics section (if present) or inserts the new statistics markup
    in an appropriate location:
      - If a statistics section exists, it is replaced.
      - Otherwise, the new statistics markup is inserted:
          * Before "==References==" if that section exists,
          * Else before "==External links==" if that section exists,
          * Else at the end of the text.
          
    The regex is designed to match from "==Statistics==" up to a new section header 
    (a line starting with "=="), or a line starting with the notes marker ('''Notes'''),
    or the end of the text.
    
    Parameters:
      old_wikitext (str): The full wikitext content extracted from the page.
      new_stats_markup (str): The new statistics markup string (should start with "==Statistics==").
      
    Returns:
      str: The updated wikitext with the statistics section updated or inserted.
    """
    # Define a pattern that captures the entire statistics block.
    # The pattern matches:
    #   - Starts with "==Statistics=="
    #   - Followed by any characters (non-greedily)
    #   - Up to (but not including) a line that:
    #       a) starts with "==" (a new section header), or
    #       b) starts with the notes marker '''Notes''', or
    #       c) the end of the text.
    pattern = re.compile(r'(==Statistics==.*?)(?=^==|\n''' + re.escape("'''Notes'''") + r'|\Z)', 
                         re.DOTALL | re.MULTILINE)
    
    if pattern.search(old_wikitext):
        # If an existing statistics block is found, replace it.
        updated_wikitext = pattern.sub(new_stats_markup, old_wikitext)
    else:
        # No existing statistics block; decide where to insert the new stats.
        if "==References==" in old_wikitext:
            idx = old_wikitext.index("==References==")
            updated_wikitext = old_wikitext[:idx].rstrip() + "\n" + new_stats_markup + "\n" + old_wikitext[idx:]
        elif "==External links==" in old_wikitext:
            idx = old_wikitext.index("==External links==")
            updated_wikitext = old_wikitext[:idx].rstrip() + "\n" + new_stats_markup + "\n" + old_wikitext[idx:]
        else:
            updated_wikitext = old_wikitext.strip() + "\n" + new_stats_markup
    return updated_wikitext
def generate_wiki_markup(data, player_name, player_url, end_round=None, end_year=None):
    """
    Generate the wiki markup string using JSON data. If end_round or end_year
    are not provided, infer them from the last entry of stats_df.
    """
    stats_df = data["stats_df"]
    averages_df = data["averages_df"]
    votes_dict = data["votes_df"][0]

    # Infer end_year and end_round from the last stats entry if not provided.
    last_entry = stats_df[-1]
    if end_year is None:
        end_year = last_entry["Season"]
    if end_round is None:
        # Assuming the "Games" field for the last season represents the last completed round.
        end_round = last_entry["Games"]

    header = (
        "\n==Statistics==\n"
        f": ''Statistics are correct to the end of {end_year}''"
        f"<ref>{{{{cite web|url={player_url}|title=AFL Tables – {player_name} statistics|website=AFL Tables}}}}</ref>\n"
        "{{AFL player statistics legend|s=y}}\n"
        "{{AFL player statistics start with votes}}\n"
    )

    body = header
    stats_keys = ["No.", "Games", "G", "B", "K", "H", "D", "M", "T"]
    avg_keys   = ["G", "B", "K", "H", "D", "M", "T"]

    for i, stat in enumerate(stats_df):
        season = stat["Season"]
        avg = next((a for a in averages_df if a["Season"] == season), {})
        # Alternate row background for clarity
        if i % 2 == 0:
            body += '|- style="background:#eaeaea;"\n'
        else:
            body += "|-\n"
        
        # Create season link (e.g. [[2013 AFL season|2013]])
        season_link = f"[[{season} AFL season|{season}]]"
        body += f"! scope=\"row\" style=\"text-align:center\" | {season_link}\n"
        
        # Use the team value directly from the JSON.
        team = stat.get("Team", "")
        body += f"| style=\"text-align:center\" | {team}\n"
        
        # Append the statistics from stats_df.
        for j, key in enumerate(stats_keys):
            value = stat.get(key, "")
            separator = "| " if j == 0 else "|| "
            body += f"{separator}{value} "
        # Append the averages from averages_df.
        for key in avg_keys:
            value = avg.get(key, "")
            body += f"|| {value} "
        # Append the votes (if available).
        votes = votes_dict.get(season, 0)
        body += f"|| {votes}\n"
    
    # Append the career totals and averages.
    career_stats = data["total_career_df"][0]
    career_avgs  = data["total_career_df"][1]
    career_votes = votes_dict.get("Total Votes", 0)
    
    body += "|-\n"
    body += "! colspan=3| Career\n"
    
    career_stats_order = ["Games", "G", "B", "K", "H", "D", "M", "T"]
    for key in career_stats_order:
        value = career_stats.get(key, "")
        body += f"! {value}\n"
    
    for key in avg_keys:
        value = career_avgs.get(key, "")
        body += f"! {value}\n"
    
    body += f"! {career_votes}\n"
    body += "|}\n\n"
    
    return body


def process_player_stats(stats_df,averages_df):
    # Splitting the DataFrame into parts
    stats_df_copy = stats_df.copy()

    # Splitting the DataFrame into stats_df and total_career_df
    main_df = stats_df_copy.iloc[:-2].copy()  # Excluding the last two rows
    total_career_df = stats_df_copy.iloc[-2:].copy()  # Only the last two rows

    # Rename index of total_career_df
    total_career_df.index = ["Career Total", "Career Average"]

    # Remove unwanted columns
    main_df.drop(columns=["Votes"], inplace=True)
    total_career_df.drop(columns=["Season", "Team", "No.", "Votes"], inplace=True)

    # Creating votes_df
    votes_values = [0 if v == "" else int(v) for v in stats_df["Votes"][:-2]]  # Replace empty votes with 0
    votes_df = pd.DataFrame([votes_values], columns=stats_df["Season"][:-2])
    votes_df["Total Votes"] = sum(votes_values)  # Add total votes column

    # Remove season column from votes_df and transpose
    votes_df = votes_df.T  # Transpose for better structure
    votes_df.columns = ["Votes"]  # Rename column
    votes_df.index.name = None  # Remove index name
    averages_df = averages_df[~averages_df.index.isin(['Totals', 'Averages'])].drop(columns=['Votes'])
    averages_df.reset_index(drop=True, inplace=True)
    votes_df = votes_df.T
    # Return the three DataFrames
    return main_df, total_career_df, votes_df,averages_df




def initialize_apis():
    """Initialize API connections and configurations"""
    load_dotenv()
    
    # Pywikibot initialization
    config.usernames['wikipedia']['en'] = os.getenv("username_afll")
    site = pywikibot.Site('en', 'wikipedia')
    site.login()
    
    return site

def update_wikipedia_page(player_name, json_data,  site, dob):
    try:
        print(f"{player_name} {dob}")
        
        page = pywikibot.Page(site, player_name)
        
        if not page.exists():
            print(f"The page '{player_name}' does not exist.")
            print("Searching with other method")
            page = pywikibot.Page(site, f"{player_name} (footballer)")
            if not page.exists():
                print("Other method not worked . Skipping")
                return False
            else:
                print("Page Found")

        criteria = {
            "must_words": ["afl"],      
            "one_of": ["footballer","football","afl"]            
        }
        current_content = page.text
        if advanced_search(current_content, **criteria):
            print("Content matches the advanced search criteria!")
        else:
            print("Content does not match the criteria.")
            return False

        stats_text=generate_wiki_markup(json_data, player_name, site)
        updated_content=update_or_insert_statistics_section_in_wikitext(current_content, stats_text)
        page.text = updated_content
        page.save(summary='Updated player statistics')
        
        print(f"Successfully updated page for {player_name}")
        return True
        
    except Exception as e:
        print(f"Error updating page for {player_name}: {str(e)}")
        return False

def advanced_search(text, must_words=None, one_of=None):
    must_words = must_words or []
    one_of = one_of or []
    for word in must_words:
            if word.lower() not in text.lower():
                return False
    if one_of and not any(word.lower() in text.lower() for word in one_of):
        return False
    return True
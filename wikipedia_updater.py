from pywikibot import config
import pywikibot
from dotenv import load_dotenv
import os
import json
from datetime import datetime
import pandas as pd
import logging
import re

def update_or_insert_statistics_section_in_wikitext(old_wikitext, new_stats_markup):
    pattern = re.compile(r'(==Statistics==.*?)(?=^==|\n''' + re.escape("'''Notes'''") + r'|\Z)', 
                         re.DOTALL | re.MULTILINE)
    
    if pattern.search(old_wikitext):
        updated_wikitext = pattern.sub(new_stats_markup, old_wikitext)
    else:
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
    try:
        stats_df = data["stats_df"]
        averages_df = data["averages_df"]
        votes_dict = data["votes_df"][0]

        last_entry = stats_df[-1]
        if end_year is None:
            end_year = last_entry["Season"]
        if end_round is None:
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
            if i % 2 == 0:
                body += '|- style="background:#eaeaea;"\n'
            else:
                body += "|-\n"
            
            season_link = f"[[{season} AFL season|{season}]]"
            body += f"! scope=\"row\" style=\"text-align:center\" | {season_link}\n"
            
            team = stat.get("Team", "")
            body += f"| style=\"text-align:center\" | {team}\n"
            
            for j, key in enumerate(stats_keys):
                value = stat.get(key, "")
                separator = "| " if j == 0 else "|| "
                body += f"{separator}{value} "
            
            for key in avg_keys:
                value = avg.get(key, "")
                body += f"|| {value} "
            
            votes = votes_dict.get(season, 0)
            body += f"|| {votes}\n"
        
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
    except Exception as e:
        logging.error(f"Error generating wiki markup: {str(e)}")
        return None

def process_player_stats(stats_df, averages_df):
    try:
        stats_df_copy = stats_df.copy()
        main_df = stats_df_copy.iloc[:-2].copy()
        total_career_df = stats_df_copy.iloc[-2:].copy()
        total_career_df.index = ["Career Total", "Career Average"]
        
        main_df.drop(columns=["Votes"], inplace=True)
        total_career_df.drop(columns=["Season", "Team", "No.", "Votes"], inplace=True)
        
        votes_values = [0 if v == "" else int(v) for v in stats_df["Votes"][:-2]]
        votes_df = pd.DataFrame([votes_values], columns=stats_df["Season"][:-2])
        votes_df["Total Votes"] = sum(votes_values)
        
        votes_df = votes_df.T
        votes_df.columns = ["Votes"]
        votes_df.index.name = None
        averages_df = averages_df[~averages_df.index.isin(['Totals', 'Averages'])].drop(columns=['Votes'])
        averages_df.reset_index(drop=True, inplace=True)
        votes_df = votes_df.T
        
        main_df = main_df.applymap(lambda v: 0 if v == "" else v)
        averages_df = averages_df.applymap(lambda v: 0 if v == "" else v)
        return main_df, total_career_df, votes_df, averages_df
    except Exception as e:
        logging.error(f"Error processing player stats: {str(e)}")
        return None, None, None, None

def initialize_apis():
    try:
        load_dotenv()
        config.usernames['wikipedia']['en'] = os.getenv("username_afll")
        site = pywikibot.Site('en', 'wikipedia')
        site.login()
        return site
    except Exception as e:
        logging.error(f"Error initializing APIs: {str(e)}")
        return None

def update_wikipedia_page(player_name, json_data, site, dob):
    try:
        logging.info(f"Updating Wikipedia page for {player_name} {dob}")
        
        page = pywikibot.Page(site, player_name)
        try:
            page.text  # Force page load with default timeout
        except pywikibot.exceptions.NoPage:
            logging.info(f"Page '{player_name}' does not exist, trying footballer suffix")
            page = pywikibot.Page(site, f"{player_name} (footballer)")
            try:
                page.text
            except pywikibot.exceptions.NoPage:
                logging.warning("Page not found with footballer suffix")
                return False

        criteria = {
            "must_words": ["afl"],      
            "one_of": ["footballer", "football", "afl"]            
        }
        
        current_content = page.text
        if advanced_search(current_content, **criteria):
            logging.info("Content matches the advanced search criteria")
        else:
            logging.warning("Content does not match the criteria")
            return False

        stats_text = generate_wiki_markup(json_data, player_name, site)
        if stats_text is None:
            return False
            
        updated_content = update_or_insert_statistics_section_in_wikitext(current_content, stats_text)
        
        # Add timeout to save operation
        try:
            page.text = updated_content
            page.save(summary='Updated player statistics')
            logging.info(f"Successfully updated page for {player_name}")
            return True
        except pywikibot.exceptions.TimeoutError:
            logging.error(f"Timeout while saving page for {player_name}")
            return False
            
    except Exception as e:
        logging.error(f"Error updating page for {player_name}: {str(e)}")
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
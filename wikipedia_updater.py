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
    
    # Extract special formatting from old content
    special_formatting = extract_special_formatting(old_wikitext)
    
    # Apply special formatting to new content
    if special_formatting:
        new_stats_markup = apply_special_formatting(new_stats_markup, special_formatting)
    
    if pattern.search(old_wikitext):
        updated_wikitext = pattern.sub(new_stats_markup, old_wikitext)
        
        # Preserve notes section if it exists
        notes_pattern = re.compile(r'(''' + re.escape("'''Notes'''") + r'.*?)(?=^==|\Z)', re.DOTALL | re.MULTILINE)
        notes_match = notes_pattern.search(old_wikitext)
        if notes_match:
            notes_section = notes_match.group(0)
            # Check if notes section is already in the updated content
            if notes_section not in updated_wikitext:
                # Find where to insert notes (before References or External links or at the end)
                if "==References==" in updated_wikitext:
                    idx = updated_wikitext.index("==References==")
                    updated_wikitext = updated_wikitext[:idx] + notes_section + "\n\n" + updated_wikitext[idx:]
                elif "==External links==" in updated_wikitext:
                    idx = updated_wikitext.index("==External links==")
                    updated_wikitext = updated_wikitext[:idx] + notes_section + "\n\n" + updated_wikitext[idx:]
                else:
                    # Make sure there's a newline after the statistics table
                    if not updated_wikitext.endswith('\n\n'):
                        if updated_wikitext.endswith('\n'):
                            updated_wikitext = updated_wikitext + '\n'
                        else:
                            updated_wikitext = updated_wikitext + '\n\n'
                    updated_wikitext = updated_wikitext + notes_section
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

def extract_special_formatting(wikitext):
    """
    Extract special formatting from the existing wikitext:
    - Bold text for leading players (bgcolor=CAE1FF with '''value''')
    - Special year formatting (bgcolor=F0E68C)
    - Notes ({{efn|...}})
    """
    special_formatting = {
        'leading_stats': {},  # Format: {season: {stat_key: value}}
        'special_years': {},  # Format: {season: formatting}
        'notes': {}           # Format: {season: note_text}
    }
    
    # Extract leading stats (bold text with bgcolor)
    leading_pattern = leading_pattern = re.compile(r'\|\s*bgcolor=CAE1FF\s*\|\s*\'\'\'([^<]*?)\'\'\'(?:<sup>†</sup>)?')
    leading_matches = leading_pattern.finditer(wikitext)
    
    # Find season context for each match
    season_pattern = re.compile(r'\|\s*\[\[(\d{4})[^\]]*\]\]')
    special_year_pattern = re.compile(r'\|\s*bgcolor=F0E68C\s*\|\s*\'\'\'(?:\[\[(\d{4})[^\]]*\]\])\'\'\'(?:<sup>#</sup>)?')
    notes_pattern = re.compile(r'\[\[(\d{4})[^\]]*\]\]({{efn\|[^}]*}})')
    
    # Process the wikitext line by line to associate seasons with stats
    lines = wikitext.split('\n')
    current_season = None
    
    for i, line in enumerate(lines):
        # Check for regular season
        season_match = season_pattern.search(line)
        if season_match:
            current_season = season_match.group(1)
            
            # Check for notes in this season
            notes_match = notes_pattern.search(line)
            if notes_match:
                note_text = notes_match.group(2)
                special_formatting['notes'][current_season] = note_text
            elif '{{efn|' in line:
                note_start = line.find('{{efn|')
                note_end = line.find('}}', note_start)
                if note_end > note_start:
                    note_text = line[note_start:note_end+2]
                    special_formatting['notes'][current_season] = note_text
        
        # Check for special year formatting
        special_year_match = special_year_pattern.search(line)
        if special_year_match:
            current_season = special_year_match.group(1)
            # Store the entire line including the team info
            special_formatting['special_years'][current_season] = line.strip()
            # If there's a note in the next line, associate it with this season
            if i + 1 < len(lines) and '{{efn|' in lines[i + 1]:
                note_start = lines[i + 1].find('{{efn|')
                note_end = lines[i + 1].find('}}', note_start)
                if note_end > note_start:
                    note_text = lines[i + 1][note_start:note_end+2]
                    special_formatting['notes'][current_season] = note_text
        
        # If we have a current season, check for leading stats
        if current_season and 'bgcolor=CAE1FF' in line:
            # Initialize dict for this season if not exists
            if current_season not in special_formatting['leading_stats']:
                special_formatting['leading_stats'][current_season] = {}
                
            # Extract the stat value and position
            leading_match = leading_pattern.search(line)
            if leading_match:
                # Determine which stat this is based on position in the line
                # This is approximate and may need refinement
                stat_value = leading_match.group(1).strip()
                
                # Count the number of "||" before this match to determine the stat type
                line_before_match = line[:leading_match.start()]
                separator_count = line_before_match.count('||')
                
                # Map separator count to stat key (approximate mapping)
                stat_keys = ["No.", "Games", "G", "B", "K", "H", "D", "M", "T", "G_avg", "B_avg", "K_avg", "H_avg", "D_avg", "M_avg", "T_avg", "Votes"]
                if separator_count < len(stat_keys):
                    stat_key = stat_keys[separator_count]
                    special_formatting['leading_stats'][current_season][stat_key] = stat_value
    
    return special_formatting

def apply_special_formatting(new_markup, special_formatting):
    """
    Apply the extracted special formatting to the new markup
    """
    lines = new_markup.split('\n')
    updated_lines = []
    current_season = None
    skip_next_line = False
    
    for i, line in enumerate(lines):
        if skip_next_line:
            skip_next_line = False
            continue
            
        # Check if this line contains a season
        season_match = re.search(r'\|\s*\[\[(\d{4})[^\]]*\]\]', line)
        
        if season_match:
            current_season = season_match.group(1)
            
            # Apply special year formatting if exists
            if current_season in special_formatting['special_years']:
                # Get the original line's statistics
                stats_parts = line.split('||')
                if len(stats_parts) > 1:
                    # Extract the special formatting but keep our season and stats
                    special_year_line = special_formatting['special_years'][current_season]
                    season_end_idx = special_year_line.find(']]') + 2
                    
                    # Add closing ''' and <sup>#</sup> if they exist in special formatting
                    season_part = special_year_line[:season_end_idx]
                    if "'''" in special_year_line and not season_part.endswith("'''"):
                        season_part += "'''"
                    if "<sup>#</sup>" in special_year_line:
                        season_part += "<sup>#</sup>"
                    
                    # Combine special formatting for season with existing stats
                    line = season_part + ' ||' + '||'.join(stats_parts[1:])
                
                # Skip the next line (team info) as it's included in the special formatting
                if i + 1 < len(lines) and "{{AFL Col}}" in lines[i + 1]:
                    skip_next_line = True
            
            # Apply notes if exists
            if current_season in special_formatting['notes']:
                note_text = special_formatting['notes'][current_season]
                # Insert the note after the season
                season_end = line.find(']]')
                if season_end > 0:
                    line = line[:season_end+2] + note_text + line[season_end+2:]
        
        # If we have a current season with leading stats, apply them
        if current_season and current_season in special_formatting['leading_stats']:
            leading_stats = special_formatting['leading_stats'][current_season]
            
            # For each stat in leading_stats, check if it's in this line
            for stat_key, stat_value in leading_stats.items():
                # Different handling for regular stats vs averages
                if '_avg' in stat_key:
                    base_key = stat_key.split('_')[0]
                    # Find the average value in the line (after the regular stat)
                    avg_pattern = re.compile(r'\|\|\s*(\d+\.\d+)\s*(?=\|\|)')
                    matches = list(avg_pattern.finditer(line))
                    
                    # Map stat keys to positions in the averages section
                    avg_keys = ["G", "B", "K", "H", "D", "M", "T"]
                    if base_key in avg_keys:
                        idx = avg_keys.index(base_key)
                        if idx < len(matches):
                            # Replace with formatted version
                            match = matches[idx]
                            line = line[:match.start()] + '|| bgcolor=CAE1FF | ' + f"'''{stat_value}'''<sup>†</sup>" + line[match.end():]
                else:
                    # Regular stats
                    stat_keys = ["No.", "Games", "G", "B", "K", "H", "D", "M", "T"]
                    if stat_key in stat_keys:
                        idx = stat_keys.index(stat_key)
                        
                        # Find the stat value in the line
                        if idx == 0:
                            # First stat has different format
                            stat_pattern = re.compile(r'\|\s*(\d+)\s*(?=\|\|)')
                        else:
                            # Other stats
                            stat_pattern = re.compile(r'\|\|\s*(\d+)\s*(?=\|\|)')
                        
                        matches = list(stat_pattern.finditer(line))
                        if idx < len(matches):
                            # Replace with formatted version
                            match = matches[idx]
                            line = line[:match.start()] + ('| ' if idx == 0 else '|| ') + 'bgcolor=CAE1FF | ' + f"'''{stat_value}'''<sup>†</sup>" + line[match.end():]
                    elif stat_key == "Votes":
                        # Votes are at the end of the line
                        votes_pattern = re.compile(r'\|\|\s*(\d+)\s*$')
                        match = votes_pattern.search(line)
                        if match:
                            line = line[:match.start()] + '|| bgcolor=CAE1FF | ' + f"'''{stat_value}'''<sup>†</sup>"
        
        # Special handling for career row
        if "class=sortbottom" in line:
            # Keep the next line (career header) as is
            updated_lines.append(line)
            if i + 1 < len(lines):
                updated_lines.append(lines[i + 1])
                skip_next_line = True
        else:
            updated_lines.append(line)
    
    return '\n'.join(updated_lines)

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
            "==Statistics==\n"
            f"''Updated to the end of the {end_year} season''."
            f"<ref>{{{{cite web|url={player_url}|title={player_name}|publisher=AFL Tables|access-date={datetime.now().strftime('%d %B %Y')}}}}}</ref>\n\n"
            "{{AFL player statistics legend|p=y}}\n"
            "{{AFL player statistics start with votes}}\n"
        )

        body = header
        stats_keys = ["No.", "Games", "G", "B", "K", "H", "D", "M", "T"]
        avg_keys   = ["G", "B", "K", "H", "D", "M", "T"]

        for i, stat in enumerate(stats_df):
            season = stat["Season"]
            avg = next((a for a in averages_df if a["Season"] == season), {})
            
            body += "|-\n"  # Always start with |-
            
            season_link = f"[[{season} AFL season|{season}]]"
            body += f"| {season_link}"  # No scope="row" or style attributes
            
            team = stat.get("Team", "")
            body += f" || {team}"  # Use || instead of | for team
            
            for j, key in enumerate(stats_keys):
                value = stat.get(key, "")
                body += f" || {value}"  # Use || for all stats
            
            for key in avg_keys:
                value = avg.get(key, "")
                body += f" || {value}"
            
            votes = votes_dict.get(season, 0)
            body += f" || {votes}\n"
        
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
        
        main_df = main_df.map(lambda v: 0 if v == "" else v)
        averages_df = averages_df.map(lambda v: 0 if v == "" else v)
        total_career_df=total_career_df.map(lambda v: 0 if v == "" else v)
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
    
def fetch_afl_player_page(site, player_name):
    """
    Tries to fetch a Wikipedia page for an AFL player.
    If the page doesn't exist, it appends (footballer) and then (Australian footballer) as fallbacks.
    """
    name_variants = [player_name, f"{player_name} (footballer)", f"{player_name} (Australian footballer)"]

    criteria = {
        "must_words": ["afl"],      
        "one_of": ["footballer", "football", "afl"]            
    }

    for name in name_variants:
        page = pywikibot.Page(site, name)
        try:
            current_content = page.text  # Attempt to load page content
            if advanced_search(current_content, **criteria):
                logging.info(f"Page '{name}' matches the criteria.")
                return page  # Return the valid page
            else:
                logging.warning(f"Page '{name}' does not match the criteria.")
        except pywikibot.exceptions.NoPage:
            logging.info(f"Page '{name}' does not exist, trying next variant.")
    
    logging.warning("No valid page found for the player.")
    return None  # No valid page found


def update_wikipedia_page(player_name, json_data, site, dob):
    try:
        logging.info(f"Updating Wikipedia page for {player_name} {dob}")
        page = fetch_afl_player_page(site,player_name)
        if page is None:
            logging.warning(f"No valid page found for {player_name}")
            return False
        
        current_content = page.text
        
        stats_text = generate_wiki_markup(json_data, player_name, site)
        if stats_text is None:
            return False
            
        updated_content = update_or_insert_statistics_section_in_wikitext(current_content, stats_text)
        with open("output.txt", "w", encoding="utf-8") as file:
            file.write(updated_content)
        print("File saved successfully!")
        input("Press Enter to continue...")
        # Check if content has actually changed before updating
        if updated_content == current_content:
            print(f"No changes detected for {player_name}'s page - skipping update")
            logging.info(f"No changes detected for {player_name}'s page - skipping update")
            return True
            
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
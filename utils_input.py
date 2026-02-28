import re
import json
import sys

import pandas as pd

def get_typed_input(prompt, target_type, default=None, display_default=True):
    """
    Asks for user input and converts it to a specific target_type.
    Supported target_types: 'str', 'int', 'float', 'date'
    """
    if display_default:
        user_val = input(f"{prompt} (Default: {default}): ").strip()
    else:
        user_val = input(f"{prompt}").strip()

    # 1. Handle Empty Input immediately
    if not user_val:
        # Ensure the default itself is a Timestamp if it's a date type
        if target_type == 'date' and default is not None:
            return pd.to_datetime(default).normalize()
        return default

    try:
        if target_type == 'int':
            return int(user_val)
        
        elif target_type == 'float':
            return float(user_val)
        
        elif target_type == 'date':
            # Assumes YYYY-MM-DD format
            # Use Pandas to parse the string directly into a Timestamp
            # .normalize() ensures time is exactly 00:00:00
            return pd.to_datetime(user_val).normalize()
        
        elif target_type == 'str':
            return str(user_val)
        
        else:
            print(f"Unknown type '{target_type}'. Returning as string.")
            return str(user_val)

    except ValueError:
        print(f"Error: Could not convert '{user_val}' to {target_type}.")
        return default


def parse_pid_input(user_input):
    """
    Parses a string for integers (PIDs) separated by any non-digit characters.
    Returns:
        - A list of unique integers (PIDs) if successful.
        - An empty list [] if the input is empty or whitespace.
        - None if an error occurs during parsing.
    """
    # 1. Check for empty input immediately
    if not user_input or str(user_input).strip() == "":
        return [], ""

    try:
        # 2. Use regex to find all sequences of digits
        # \d+ matches one or more digits
        raw_matches = re.findall(r'\d+', str(user_input))
        
        # 3. Convert to unique integers and SORT them
        # Sorting is crucial if this string is used for memory-keying
        pids = sorted(list(set(int(m) for m in raw_matches)))

        # 4. Create the cleaned string (PIDs separated by a single space)
        cleaned_str = " ".join(map(str, pids))
        
        return pids, cleaned_str
    
    except Exception as e:
        print(f"Error parsing PIDs: {e}")
        return None, None 
    
def get_parameters(file_name):
    # Input parameters are stored in dictionary "inputs"
    with open(file_name, 'r') as input_file:
        inputs = json.load(input_file)
        return inputs
    
    print(f'Failed to open "{file_name}"')
    return {}

def parse_smart_selection(usr_input, current_df):
    usr_input = usr_input.strip().lower()
    max_count = len(current_df)

    if usr_input == 'all':
        return list(range(max_count))

    indices = []
    # Replace commas with spaces so '1, 2, 3' also works
    parts = usr_input.replace(',', ' ').split()
    
    for part in parts:
        if '-' in part:
            try:
                start_str, end_str = part.split('-', 1)
                start, end = int(start_str), int(end_str)
                
                # Clamp the values to the actual size of the dataframe
                # 1-indexed to 0-indexed conversion happens here
                actual_start = max(0, start - 1)
                actual_end = min(max_count, end) 
                
                indices.extend(range(actual_start, actual_end))
            except ValueError:
                continue
        else:
            try:
                idx = int(part)
                if 1 <= idx <= max_count:
                    indices.append(idx - 1)
            except ValueError:
                continue
                
    return sorted(list(set(indices))) # Sorted makes processing later much easier

# Make simple progress bar
def progress_bar(iteration, total, prefix='', suffix='', length=30, fill='█'):
    percent = f"{100 * (iteration / float(total)):.1f}"
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    
    # \r returns the cursor to the start of the line
    sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {suffix}')
    sys.stdout.flush()
    

def generate_next_pid(pid_series, prefix):
    # Standardize padding length (e.g., 5 for 00001)
    padding = 5 
    
    if pid_series.empty:
        return f"{prefix}{1:0{padding}d}"
        
    valid_pids = pid_series.dropna()
    # Ensure we only look at IDs with the specific prefix provided
    valid_pids = valid_pids[valid_pids.str.startswith(prefix, na=False)]

    if valid_pids.empty:
        return f"{prefix}{1:0{padding}d}"

    # FIX: Use len(prefix) to slice correctly regardless of prefix length
    # This peels off 'p', 'tc', or 'anything' accurately
    numeric_parts = valid_pids.str[len(prefix):].str.extract(r'(\d+)')[0]
    
    next_number = pd.to_numeric(numeric_parts, errors='coerce').max() + 1
    
    if pd.isna(next_number):
        return f"{prefix}{1:0{padding}d}"

    return f"{prefix}{int(next_number):0{padding}d}"

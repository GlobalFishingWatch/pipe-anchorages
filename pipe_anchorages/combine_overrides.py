# %%
import sys
import argparse
from s2sphere import CellId, LatLng
import numpy as np
import pandas as pd

# %%
PORT_LIST_FLDR = './data/port_lists'

# %%
def s2_anchorage_style(lat: float, lon: float) -> str:
    # Build the level-14 S2 cell containing this point and return the first 8 hex digits as a string
    cid = CellId.from_lat_lng(LatLng.from_degrees(lat, lon)).parent(14)
    # Format the 64-bit id as 16 hex chars and take the first 8 (most significant)
    return f"{cid.id():016x}"[:8]


# %%
def clean_overrides(df, duplicate_option = 'nothing'):
    duplicate_options = ['keep_last', 'combine_with_ampersand','nothing']
    if duplicate_option not in duplicate_options:
        raise Exception(f"{duplicate_option} not a valid duplicate_option")

    # fix messed up s2ids
    messed_up_s2id_count = 0
    for idx, row in df.iterrows():
        s2id = str(row['s2id'])
        if ('+' in s2id) or (len(s2id) != 8):
            df.loc[idx, 's2id'] = s2_anchorage_style(row['latitude'], row['longitude'])
            messed_up_s2id_count = messed_up_s2id_count+1
    print(f"Fixed {messed_up_s2id_count} messed up s2ids")

    # handle duplicates

    old_len = len(df)

    dupes = df[df.duplicated(subset='s2id', keep=False)]
    print(f"There are {len(dupes)} duplicate rows across {len(np.unique(dupes['s2id']))} s2ids")
    if len(dupes) > 0:
        print(f"\n ** START DUPLICATE INFO ** ")
        
        dupes1 = dupes[['s2id','latitude','longitude']]

        conflicting_ids = (
            dupes1.groupby('s2id')
            .nunique(dropna=False)          # count distinct values per column
            .gt(1)                          # flag columns with >1 unique value
            .any(axis=1)                    # True if any column differs
        )
        conflicting_ids = conflicting_ids[conflicting_ids].index

        # Filter and order
        conflicting_rows = dupes1[dupes1['s2id'].isin(conflicting_ids)]
        print(f"Different lat/lons: {len(conflicting_rows)} duplicate rows with {len(np.unique(conflicting_rows['s2id']))} s2ids")


        dupes1 = dupes[['s2id','label','sublabel','iso3']]

        conflicting_ids = (
            dupes1.groupby('s2id')
            .nunique(dropna=False)          # count distinct values per column
            .gt(1)                          # flag columns with >1 unique value
            .any(axis=1)                    # True if any column differs
        )
        conflicting_ids = conflicting_ids[conflicting_ids].index

        # Filter and order
        conflicting_rows = dupes1[dupes1['s2id'].isin(conflicting_ids)].sort_values(by='s2id')
        print(f"Different labels/sublabels: {len(conflicting_rows)} duplicate rows with {len(np.unique(conflicting_rows['s2id']))} s2ids:")
        print(conflicting_rows)


    if duplicate_option == 'keep_last':
        df = df.drop_duplicates(subset='s2id', keep='last').reset_index(drop=True)
        print(f"Dropped {old_len - len(df)} duplicates")
        if len(dupes) > 0:
            print(f"** END DUPLICATE INFO ** \n")
    elif duplicate_option == 'combine_with_ampersand':
        # Filter to duplicated s2ids (includes all occurrences)
        dupes = df[df.duplicated(subset='s2id', keep=False)]

        # Group by s2id so you can loop through each set of duplicates

        n_labels_combined = 0
        n_sublabels_combined = 0

        for s2id, group in dupes.groupby('s2id'):
            max_idx = group.index.max()

            if len(np.unique(group['label'])) > 1:
                s = ''
                for x in np.unique(group['label']):
                    if pd.isna(x):
                        pass
                    elif len(s) == 0:
                        s = x
                    else:
                        s = f"{s} & {x}"
                if len(s) > 0:
                    df.loc[max_idx, 'label'] = s
                    n_labels_combined = n_labels_combined + len(group) - 1

            if group['sublabel'].nunique(dropna=False) > 1: # can handle None
                s = ''
                for x in np.unique(group['sublabel']):
                    if pd.isna(x):
                        pass
                    elif len(s) == 0:
                        s = x
                    else:
                        s = f"{s} & {x}"
                if len(s) > 0:
                    df.loc[max_idx, 'sublabel'] = s
                    n_sublabels_combined = n_sublabels_combined + len(group) - 1

            idxs = list(group.index)
            idxs.remove(max(idxs))
                
            df = df.drop(index=idxs)
            
        df = df.reset_index(drop=True)
        print(f"Handled {old_len - len(df)} duplicates")
        print(f"Rows whose labels were combined: {n_labels_combined}")
        print(f"Rows whose sublabels were combined: {n_sublabels_combined}")
    
    elif duplicate_option == 'nothing':
        dupes = df[df.duplicated(subset='s2id', keep=False)]
        if len(dupes) > 0:
            print(f"WARNING: There are {len(dupes)} duplicated s2ids that were not handled")
        else:
            print(f"There are 0 duplicated s2ids")

    else:
        raise Exception("invalid duplicate option, should be unreachable")
        
    return(df)


# %%
def run(args=None):

    parser.add_argument(
        "--output_filename",
        type=str,
        required=True,           # force the user to supply it
        help="Name of the output file"
    )

    parsed = parser.parse_args(args[1:])   # skip program name
    print("Output file:", parsed.output_file)

    overrides_filename = f"{PORT_LIST_FLDR}/overrides_file_order.txt"
    combined_anchorages = []
    with open(overrides_filename, "r") as file:
        for i, line in enumerate(file):
            line = line.strip()

            if i == 0:
                # first line
                print(f"\nAdding overrides from {line}...")
                combined_anchorages = pd.read_csv(f"{PORT_LIST_FLDR}/{line}")
                combined_anchorages = clean_overrides(combined_anchorages, duplicate_option='keep_last')
                combined_anchorages['source']=line
            else:
                print(f"\nAdding overrides from {line}...")
                df = pd.read_csv(f"{PORT_LIST_FLDR}/{line}")
                df = clean_overrides(df, duplicate_option='keep_last')
                df['source'] = line

                combined_anchorages = pd.concat([combined_anchorages,df])
                mask = combined_anchorages.duplicated(subset='s2id', keep='last')
                dropped = combined_anchorages[mask].copy() 
                old_len = len(combined_anchorages)
                combined_anchorages = combined_anchorages[~mask].reset_index(drop=True)
                overwrite_count = old_len - len(combined_anchorages)
                if overwrite_count > 0:
                    print(f"** OVERWROTE {old_len - len(combined_anchorages)} s2ids FROM PREVIOUS OVERRIDE FILES **")
                    drop_counts = dropped['source'].value_counts()
                    for source, count in drop_counts.items():
                        print(f"- {count} s2ids from {source} were overwritten")
    
    combined_anchorages.to_csv(f"{PORT_LIST_FLDR}/{parsed.output_filename}")


# %%
if __name__ == "__main__":
    sys.exit(run(args=sys.argv))

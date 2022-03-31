import pandas as pd
import os
import sys
import argparse
import importlib.util
from datetime import datetime
from numpy import intersect1d
from pathlib import Path
from functools import reduce

NOW = datetime.now().strftime("%Y%m%d%H%M%S")

def track_reports(general_path, write_tracker):
    """Reads report folders in the path, creates dataframe and saves it to the tracker file"""

    tracker_path = GENERAL_PATH / f"{NOW}_tracker.csv"
    try:
        tracker = pd.read_csv(tracker_path, delimiter=";")
        tracked_reports = list(tracker["report_name"])
    except (pd.errors.EmptyDataError, FileNotFoundError) as err:
        tracker = pd.DataFrame()
        tracked_reports = []

    reports_data = []
    for file in os.listdir(GENERAL_PATH):
        
        file_path = GENERAL_PATH / file
        if file_path.is_dir() and file[0].isdigit():
            if int(file) in tracked_reports:
                continue
            else:
                reports_data.append(collect_report(file_path))

    if reports_data:
        updated_tracker = pd.concat([tracker, pd.DataFrame(reports_data)]).set_index("report_name") 
        if params.SEARCH_REPEATED:
            common_samples = reduce(intersect1d, [list(val) for val in updated_tracker["unique_samples"]])
            updated_tracker.loc[:, "repeated_samples"] = str(common_samples)
        added_reps = [rep["report_name"] for rep in reports_data]
        print(f"Untracked report(s): {added_reps} ")

        if write_tracker:
            try:
                updated_tracker.to_csv(tracker_path, sep=";")
                print(f"{tracker_path} created.")
            except Exception as write_err:
                print(track_report.__name__, write_err)
        else:
            print("Overwrite required")
    else:
        print("Reports tracker is up to date.")


def meta_parser(meta_handle):
    lines = [line.strip() for line in meta_handle.readlines()]
    meta_dict = {key: val for key, val in [line.split(": ") for line in lines]}
    return meta_dict


def collect_report(file_path):
    report_dict = dict()
    report_dict["report_name"] = file_path.name

    report_samples, report_dates = [[] for i in range(2)]


    for report_file in os.listdir(file_path):
        rep_path = file_path / report_file
        if report_file == "meta.txt":
            meta_handle = open(rep_path)
            meta_dict = meta_parser(meta_handle)
            report_dict.update(meta_dict)
        else:
            try:
                main_file = pd.read_csv(rep_path, delimiter=";")
                unique_samples = list(main_file["id"].unique())
                report_samples += unique_samples
                if params.GET_LAST_DATE:
                    file_dates = main_file["date"].values
                    report_dates += list(file_dates)
            except Exception as err:
                print("Uncaught error:", "collect_report", err)
    
    report_dict["unique_samples"] = list(set(unique_samples))
    if params.GET_LAST_DATE:
        report_dict["last_date"] = max(pd.to_datetime(report_dates, format="%d.%m.%Y")).date()
    return report_dict



if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--path", "-p", nargs="?", help="Enter path to folder with reports.", type=str)
    args_parser.add_argument("--write-to-file", "-w", nargs="?", help="True or False, default True", type=bool, default=True)
    try:
        args = args_parser.parse_args()
        GENERAL_PATH = Path(args.path)
        write_tracker = args.write_to_file

        params_path = GENERAL_PATH / "params.py"
        spec = importlib.util.spec_from_file_location("params", params_path)
        params = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(params)
        track_reports(GENERAL_PATH, write_tracker)

    except Exception as main_err:
        print(main_err)
        print(args_parser.format_help())

    
    # assert len(params) == 2, "Script accepts only 1 boolean parameter"
    # assert type(eval(sys.argv[1])) is bool, "Parameter should be True or False"
    # overwrite_tracker = eval(sys.argv[1])